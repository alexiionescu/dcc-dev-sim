use crate::log;

use serde_json::json;
use tokio::time::{sleep, Instant};
use std::net::Ipv4Addr;

use super::web_data::*;


pub struct DCareDevice {
    pub socket: tokio::net::UdpSocket,
    pub http_client: reqwest::Client,
    pub pin: u64,
    pub last_keep_alive: Instant,
    pub line_id: u64,
    pub line_name: Option<String>,
    pub db_id: u64,
    pub obj_ref: i64,
    pub status: i64,
    pub contact_id: u64,
}

impl DCareDevice {
    pub async fn new(server_addr: &str) -> Result<Self, anyhow::Error> {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.connect(server_addr).await?;
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?;
        Ok(Self {
            socket,
            http_client,
            pin: 0,
            last_keep_alive: Instant::now(),
            line_id: 0,
            line_name: None,
            db_id: 0,
            obj_ref: 0,
            status: 0,
            contact_id: 0,
        })
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
        post_request(Some(&self.http_client), server_addr, token_pid, obj, method, body).await
    }


    pub fn need_keep_alive(&mut self) -> bool {
        if self.last_keep_alive.elapsed().as_secs() >= 3 {
            self.last_keep_alive = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn send_keep_alive(&mut self) {
        self.socket
            .send(&[0x00, 0x00, 0x00, 0x00])
            .await.ok();
    }


    #[inline]
    fn get_device_id(&self) -> String {
        format!("__SIM_DCARE_{:03}", self.pin)
    }

    #[inline]
    fn get_device_name(&self) -> String {
        format!("DcareEX Sim {:03}", self.pin)
    }

    pub async fn initialize(
        &mut self,
        pin: u64,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        self.pin = pin;
        self.request_dcare_line_id(server_addr, token_pid).await?;
        self.request_dcare_line_name(server_addr, token_pid).await?;
        self.request_device_or_new(server_addr, token_pid).await?;
        if self.status == 0 {
            self.request_start_device(server_addr, token_pid).await?;
            self.status = 1;
        }
        self.request_contact_id(server_addr, token_pid).await?;
        if self.contact_id == 0 {
            self.request_sign_in(server_addr, token_pid).await?;
        }
        self.last_keep_alive = Instant::now();
        Ok(())
    }

    pub async fn deinitialize(
        &mut self, 
        server_addr: &str,
        token_pid: &str
    ) -> Result<(), anyhow::Error> {
        if self.contact_id > 0 {
            self.request_sign_out(server_addr, token_pid).await?;
        }
        if self.status == 1 {
            self.request_stop_device(server_addr, token_pid).await?;
        }
        Ok(())
    }

    
    async fn request_sign_in(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        match self
        .post_request(
            server_addr,
            token_pid,
            ObjTypeOrRef::Type("DCC.Contacts"),
            "Login",
            format!(r#"{{"IdEndpoint":{},"PIN":"{}"}}"#, self.db_id, self.pin),
        ) 
        .await? {
            PoltysResponse::Err(poltys_response_error) => {
                Err(anyhow::anyhow!("login failed: {poltys_response_error}"))
            }
            PoltysResponse::Other(val) => {
                self.contact_id = val.as_u64().unwrap_or_else(|| {
                    log!(2, "[DCare_{0:03}] Contact Login response is not a valid number: {val}", self.pin);
                    0
                });
                log!(3, "[DCare_{:03}] Contact Login OK -> contact_id: {:04}", self.pin, self.contact_id);
                Ok(())
            }
            o => {
                Err(anyhow::anyhow!("Contact Login unexpected response type {o:?}"))
            }
        }
       
    }
    async fn request_sign_out(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC.Contacts"),
                "Logout",
                format!(r#"{{"IdEndpoint":{}}}"#, self.db_id),
            ) 
            .await? { 
            Err(anyhow::anyhow!("login failed: {poltys_response_error}"))
        } else {
            log!(3, "[DCare_{0:03}] Contact Logout OK", self.pin);
            Ok(())
        }
    }
    
    async fn request_contact_id(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        let parse_res: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC.Contacts"),
                "IsLoggedIn",
                format!(r#"{{"IdEndpoint":{}}}"#,self.db_id),
            )
            .await?;
        self.contact_id = parse_res
            .as_object()
            .and_then(|obj| obj.get("ContactID"))
            .and_then(|v| v.as_number())
            .ok_or_else(|| { 
                anyhow::anyhow!("IsLoggedIn contact id not found in response: {parse_res}") 
            })?
            .as_u64()
            .ok_or_else(|| {
                anyhow::anyhow!( "IsLoggedIn contact id is not a valid number in response: {parse_res}" ) 
            })?;
        log!(3, "[DCare_{0:03}] IsLoggedIn OK -> contact id: {1:04}", self.pin,self.contact_id);
        Ok(())
    }
    
    async fn request_start_device(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr, 
                token_pid, 
                ObjTypeOrRef::ObjectRef(self.obj_ref), 
                "StartScript", 
                "{}".to_string()
            )
            .await? {
            Err(anyhow::anyhow!("StartScript failed: {poltys_response_error}"))
        } else {
            log!(3, "[DCare_{0:03}] StartScript OK", self.pin);
            Ok(())
        }
    }


    async fn request_stop_device(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.obj_ref),
                "StopScript",
                "{}".to_string(),
            )
            .await? {
            Err(anyhow::anyhow!("StopScript failed: {poltys_response_error}"))
        } else {
            self.status = 0;
            log!(3, "[DCare_{0:03}] StopScript OK", self.pin);
            Ok(())
        }
    }
    
    async fn request_device_or_new(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        let device_id = self.get_device_id();
        let device_name = self.get_device_name();
        let parse_res: PoltysResponse = self
        .post_request(
            server_addr,
            token_pid,
            ObjTypeOrRef::Type("Devices.Endpoints"),
            "GetDetails",
            format!(r#"{{"HWIdentifier":"{device_id}","Line":"{}","GetData":true,"GetRef":true}}"#,self.line_name.as_ref().unwrap()),
        )
        .await?;
        let dev = match parse_res {
            PoltysResponse::Err(poltys_response_error) => {
                if poltys_response_error.has_part_id("ERR_FIND")
                {
                    log!(2, "[DCare_{0:03}] device not found, creating new one", self.pin);
                    let default_alarm_types: serde_json::Value = self
                        .post_request(
                            server_addr,
                            token_pid,
                            ObjTypeOrRef::Type("Devices.Endpoints"),
                            "AlarmTypeList",
                            r#"{"Condition":"EndpointTypes.Name = 'DCareDevice'"}"#.to_string(),
                        )
                        .await?;
                    let body = self.create_endpoint_body(
                        &device_id,
                        &device_name,
                        self.line_name.as_ref().unwrap(),
                        default_alarm_types,
                    );
                    log!(4, "[DCare_{0:03}] device created body {body}", self.pin);
                    let create_res: PoltysResponse = self
                        .post_request(
                            server_addr,
                            token_pid,
                            ObjTypeOrRef::Type("Devices.Endpoints"),
                            "Create",
                            serde_json::to_string(&body)?,
                        )
                        .await?;
                    match create_res {
                        PoltysResponse::Err(poltys_response_error) => {
                            return Err(anyhow::anyhow!("create device failed: {poltys_response_error}"));
                        }
                        PoltysResponse::Other(dev) => {
                            log!(1, "[DCare_{0:03}] device created successfully. waiting for it to be ready", self.pin);
                            sleep(std::time::Duration::from_millis(500)).await; // Give some time for the device to be created
                            dev
                        }
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Unexpected response type: {:?}",
                                create_res
                            ));
                        }
                    }
                } else {
                    return Err(anyhow::anyhow!("get details failed: {poltys_response_error}"));
                }
            }
            PoltysResponse::Other(dev) => {
                log!(4, "[DCare_{0:03}] device already exists, skip creation", self.pin);
                dev
            }
            _ => {
                return Err(anyhow::anyhow!("Unexpected response type: {:?}", parse_res));
            }
        };
        log!(4, "[DCare_{0:03}] device details {dev}", self.pin);
        self.db_id = dev
            .as_object()
            .and_then(|obj| obj.get("Id"))
            .and_then(|v| v.as_number())
            .ok_or_else(|| anyhow::anyhow!("get details db id not found in response: {dev}"))?
            .as_u64()
            .ok_or_else(|| {
                anyhow::anyhow!("get details db id is not a valid number in response: {dev}")
            })?;
        self.obj_ref = dev
            .as_object()
            .and_then(|obj| obj.get("ObjectRef"))
            .and_then(|v| v.as_number())
            .ok_or_else(|| anyhow::anyhow!("get details obj ref not found in response: {dev}"))?
            .as_i64()
            .ok_or_else(|| {
                anyhow::anyhow!("get details obj ref is not a valid number in response: {dev}")
            })?;
        self.status = dev.as_object()
            .and_then(|obj| obj.get("Status"))
            .and_then(|v| v.as_number())
            .ok_or_else(|| anyhow::anyhow!("get details status not found in response: {dev}"))?
            .as_i64()
            .ok_or_else(|| {
                anyhow::anyhow!("get details status is not a valid number in response: {dev}")
            })?;
        log!(3, "[DCare_{0:03}] GetDetails OK -> DB ID: {1}, ObjectRef: {2} Status: {3}", self.pin, self.db_id, self.obj_ref, self.status);
        Ok(())
    }
    
    async fn request_dcare_line_name(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        let parse_res: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("Devices.Lines"),
                "GetDetails",
                format!(r#"{{"Id":{0},"GetData":true,"GetRef":true}}"#, self.line_id),
            )
            .await?;
        self.line_name = Some(parse_res
            .as_object()
            .and_then(|obj| obj.get("Name"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("[DCare_{0:03}] line name not found in response: {parse_res}", self.pin))?.to_string());
        log!(3, "[DCare_{0:03}] Line GetDetails OK -> Name: {1}", self.pin, self.line_name.as_ref().unwrap());
        Ok(())
    }
    
    async fn request_dcare_line_id(&mut self, server_addr: &str, token_pid: &str) -> Result<(), anyhow::Error> {
        let parse_res = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("Devices.Lines"),
                "List",
                r#"{"Condition":"LineTypes.Name = 'DCare'"}"#.to_string(),
            )
            .await?;
        self.line_id = match parse_res {
            serde_json::Value::Array(values) => match values.first() {
                Some(serde_json::Value::Array(line)) => line
                    .first()
                    .and_then(|v| v.as_number())
                    .ok_or_else(|| {
                        anyhow::anyhow!("[DCare_{0:03}] line identifier not found in response: {values:?}", self.pin)
                    })?
                    .as_u64()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "[DCare_{0:03}] line identifier is not a valid number in response: {values:?}", self.pin
                        )
                    }),
                _ => Err(anyhow::anyhow!(
                    "No DCare line found in response: {values:?}"
                )),
            },
            _ => Err(anyhow::anyhow!(
                "[DCare_{1:03}] line list: expected an array, got {:?}",
                parse_res, self.pin
            )),
        }?;
        log!(3, "[DCare_{0:03}] Lines List DCare OK -> Line ID: {1}", self.pin, self.line_id);
        Ok(())
    }
    
    fn create_endpoint_body(
        &self,
        device_id: &str,
        device_name: &str,
        line_name: &str,
        default_alarm_types: serde_json::Value,
    ) -> serde_json::Value {
        let ep_alarms = default_alarm_types.as_array()
            .map(|alarms| {
                alarms
                    .iter()
                    .filter_map(|alarm| {
                        if let (Some(alarm_id), Some(alarm_name)) = alarm.as_array().map(|a| (a.first().and_then(|v| v.as_str()), a.get(2).and_then(|v| v.as_str())))
                            .unwrap_or((None, None))
                            {
                                Some(json!({
                                    "Devices::EndpointAlarm": {
                                        "AlarmType": { "Routing::AlarmType": {"Name": alarm_name} },
                                        "EndpointAlarmType": {
                                            "Devices::EndpointAlarmType": {
                                                "EndpointType": { "Devices::EndpointType": {"Name": "DCareDevice"} },
                                                "Identifier": alarm_id
                                            }
                                        }
                                    }
                                }))
                            } 
                            else { 
                                None 
                            }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        json!({
            "New": {
                "Devices::Endpoint": {
                    "Identifier": device_name,
                    "HWIdentifier": device_id,
                    "Description": "Simulated DCare device",
                    "Line": { "Devices::Line": {"Name": line_name} },
                    "EndpointType": { "Devices::EndpointType": {"Name": "DCareDevice", "IsDestination": true} },
                    "EndpointAlarm": ep_alarms
                }
            }
        })
    }
}
