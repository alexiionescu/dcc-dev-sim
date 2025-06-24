use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload};
use http_body_util::Empty;
use hyper::{
    Request,
    header::{CONNECTION, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use serde_json::json;
use tokio::{net::TcpStream, time::Instant};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore},
};

use crate::{
    devices::utils::web_data::{ObjTypeOrRef, PoltysResponse, post_request},
    log,
};

pub struct WebClient {
    pub ws: Option<fastwebsockets::FragmentCollector<TokioIo<Upgraded>>>,
    pub http_client: reqwest::Client,
    pub last_refresh_time: Option<Instant>,
    pub new_pid: u32,
    pub pin: usize,
    pub need_refresh: bool,
    pub last_received: Option<Instant>,
    pub events_received: usize,
    pub reconnects: usize,
    pub last_report: Instant,
}

impl WebClient {
    pub fn new(pin: usize) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?;
        Ok(Self {
            ws: None,
            http_client,
            last_refresh_time: None,
            last_received: None,
            new_pid: 0,
            pin,
            need_refresh: false,
            events_received: 0,
            reconnects: 0,
            last_report: Instant::now(),
        })
    }

    pub async fn initialize(&mut self, server_addr: &str) -> Result<()> {
        self.need_refresh = true;
        self.last_received = Some(Instant::now());
        let mut ws = connect(server_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect WebSocket: {}", e))?;
        let frame = Frame::new(
            true,
            OpCode::Text,
            None,
            Payload::Bytes(BytesMut::from(
                Self::get_connect_bulk().to_string().as_bytes(),
            )),
        );
        ws.write_frame(frame).await?;
        self.ws = Some(ws);
        Ok(())
    }

    pub async fn deinitialize(&mut self, _server_addr: &str, _token_pid: &str) -> Result<()> {
        // Add any cleanup logic here, such as closing websockets or releasing resources.
        // For now, just log and return Ok.
        log!(3, "[Web_{:03}] Deinitializing device", self.pin);
        Ok(())
    }

    pub async fn process_ws_msg(
        &mut self,
        _server_addr: &str,
        _token_pid: &str,
        msg: Frame<'_>,
    ) -> Result<()> {
        // Implement your message processing logic here.
        // For now, just log the message and return Ok.
        match msg.opcode {
            OpCode::Text => {
                self.last_received = Some(Instant::now());
                if let Ok(text) = String::from_utf8(msg.payload.to_vec()) {
                    let json: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
                    if json.get("KeepAlive").is_some() {
                        log!(5, "[Web_{:03}] WS KeepAlive message received", self.pin);
                        // Handle KeepAlive message if needed.
                    } else if json.get("EventType").is_some() {
                        log!(4, "[Web_{:03}] WS EventType received", self.pin);
                        // Handle EventType update.
                        self.events_received += 1;
                        self.need_refresh = true;
                    } else if json
                        .get("Response")
                        .is_some_and(|r| r.is_boolean() && r.as_bool().unwrap())
                    {
                        log!(3, "[Web_{:03}] WS Response received {}", self.pin, json);
                    } else {
                        anyhow::bail!("Unexpected ws text message format: {}", text);
                    }
                } else {
                    anyhow::bail!(
                        "Invalid UTF-8 sequence in WS text message. Payload length: {}",
                        msg.payload.len()
                    );
                }
            }
            OpCode::Binary => {
                anyhow::bail!(
                    "Binary WS message received, which is not expected. Payload length: {}",
                    msg.payload.len()
                );
            }
            OpCode::Close => {
                log!(2, "[Web_{:03}] WebSocket closed", self.pin);
                self.ws = None;
            }
            OpCode::Ping => {
                log!(5, "[Web_{:03}] Received WS ping", self.pin);
                // Optionally, you can respond with a Pong frame here.
                if let Some(ws) = &mut self.ws {
                    ws.write_frame(Frame::new(
                        true,
                        OpCode::Pong,
                        None,
                        Payload::Bytes(BytesMut::from("Pong response")),
                    ))
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to send Pong frame: {}", e))?;
                    log!(5, "[Web_{:03}] Sent WS pong response", self.pin);
                }
            }
            _ => {
                anyhow::bail!(
                    "Unsupported WS frame type received: {:?}, payload length: {}",
                    msg.opcode,
                    msg.payload.len()
                );
            }
        }

        Ok(())
    }

    pub async fn check_interval(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        if self.last_report.elapsed().as_secs() > 300 {
            log!(
                3,
                "[Web_{:03}] Status:{} Reporting  events_received={}, reconnects={} ",
                self.pin,
                if self.ws.is_some() {
                    "Connected"
                } else {
                    "Disconnected"
                },
                self.events_received,
                self.reconnects
            );
            self.last_report = Instant::now();
        }
        if self
            .last_received
            .is_some_and(|t| t.elapsed().as_secs() > 30)
        {
            log!(
                3,
                "[Web_{:03}] No WS messages received in the last 30 seconds. Reinitializing WebSocket.",
                self.pin
            );
            self.ws = None;
            if let Err(e) = self.initialize(server_addr).await {
                log!(
                    1,
                    "[Web_{:03}] Failed to reinitialize WebSocket: {}",
                    self.pin,
                    e
                );
            } else {
                self.reconnects += 1;
                log!(
                    2,
                    "[Web_{:03}] WebSocket reinitialized successfully",
                    self.pin
                );
            }
        }
        if self.need_refresh
            || self
                .last_refresh_time
                .is_some_and(|t| t.elapsed().as_secs() > 30)
        {
            let instant = Instant::now();
            self.need_refresh = false;
            self.last_refresh_time = Some(instant);
            self.request_refresh_active_summary(server_addr, token_pid)
                .await?;
        }

        Ok(())
    }

    fn get_connect_bulk() -> serde_json::Value {
        json!(
            {
                "Type": "ConnectBulk",
                "ConnectionsInfo": [
                  {
                    "Cmd::ConnectionInfo": {
                      "ObjectType": "DCC",
                      "ObjectName": "Alarms",
                      "EventType": "RuntimeSkillsChanged",
                      "Userdata": "ACTIVE_SUMMARY",
                      "Connect": true
                    }
                  },
                  {
                    "Cmd::ConnectionInfo": {
                      "ObjectType": "Routing",
                      "ObjectName": "Activities",
                      "EventType": "Created",
                      "Userdata": "ACTIVE_SUMMARY",
                      "Connect": true
                    }
                  },
                  {
                    "Cmd::ConnectionInfo": {
                      "ObjectType": "Routing",
                      "ObjectName": "Activities",
                      "EventType": "Closing",
                      "Userdata": "ACTIVE_SUMMARY",
                      "Connect": true
                    }
                  },
                  {
                    "Cmd::ConnectionInfo": {
                      "ObjectType": "Routing",
                      "ObjectName": "Activities",
                      "EventType": "StateChanged",
                      "Userdata": "ACTIVE_SUMMARY",
                      "Connect": true
                    }
                  },
                  {
                    "Cmd::ConnectionInfo": {
                      "ObjectType": "Routing",
                      "ObjectName": "Activities",
                      "EventType": "SubjectChanged",
                      "Userdata": "ACTIVE_SUMMARY",
                      "Connect": true
                    }
                  }
                ]
              }
        )
    }

    async fn request_refresh_active_summary(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<()> {
        let res: PoltysResponse = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC::Alarms"),
                "ActiveCount",
                r#"{"Condition":"","Having":"","Version":1}"#.to_string(),
            )
            .await?;
        if let PoltysResponse::Err(poltys_response_error) = &res {
            if poltys_response_error.error == "ERR_BAD_PROCESS_ID"
                || poltys_response_error.error == "ERR_CLIENT_NOT_READY"
            {
                self.new_pid = poltys_response_error.code;
            }
            return Err(anyhow::anyhow!(
                "request ActiveCount failed: {poltys_response_error}"
            ));
        }
        let _: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC::Alarms"),
                "GetActiveAlarmsEndpoints",
                r#"{}"#.to_string(),
            )
            .await?;
        let _: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC::Alarms"),
                "ActiveList",
                r#"{"Version":1,"Start":0,"Length":1000,"OrderC":[],"OrderT":[],"Condition":"","Having":""}"#.to_string(),
            )
            .await?;
        let _: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC::Logins"),
                "AvailableList",
                r#"{"Locations":[],"Competences":[],"Shift":"Weekend"}"#.to_string(),
            )
            .await?;
        let _: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC::Checkins"),
                "ActiveList",
                r#"{"Start":0,"Length":1000,"OrderC":[1],"OrderT":["ASC"],"Condition":"","Having":""}"#.to_string(),
            )
            .await?;
        Ok(())
    }

    async fn post_request<T>(
        &self,
        server_addr: &str,
        token_pid: &str,
        obj: ObjTypeOrRef,
        method: &str,
        body: String,
    ) -> Result<T, anyhow::Error>
    where
        T: serde::de::DeserializeOwned + std::fmt::Debug,
    {
        post_request(
            Some(&self.http_client),
            server_addr,
            token_pid,
            obj,
            method,
            body,
        )
        .await
    }
}

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

fn tls_connector() -> Result<TlsConnector> {
    let mut root_cert_store = RootCertStore::empty();
    root_cert_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
        tokio_rustls::rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject.as_ref().to_vec(),
            ta.subject_public_key_info.as_ref().to_vec(),
            ta.name_constraints.clone().map(|nc| nc.as_ref().to_vec()),
        )
    }));
    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

async fn connect(server_addr: &str) -> Result<FragmentCollector<TokioIo<Upgraded>>> {
    let tcp_stream = TcpStream::connect(&server_addr).await?;
    let tls_connector = tls_connector().unwrap();
    let host = server_addr.split(':').next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid server address")
    })?;
    let domain = tokio_rustls::rustls::ServerName::try_from(host)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dns name"))?;

    let tls_stream = tls_connector.connect(domain, tcp_stream).await?;

    let req = Request::builder()
        .method("GET")
        .uri(format!("https://{}/api.ws", &server_addr)) //stream we want to subscribe to
        .header("Host", host)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())?;

    let (ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await?;
    Ok(FragmentCollector::new(ws))
}
