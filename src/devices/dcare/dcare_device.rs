use crate::log;

use ahash::AHashSet;
use itertools::Itertools as _;
use rand::{Rng as _, distr::Alphanumeric};
use serde_json::json;
use std::net::Ipv4Addr;
use tokio::time::{Instant, sleep};

use crate::devices::utils::web_data::*;

pub struct DCareDevice {
    pub socket: tokio::net::UdpSocket,
    pub http_client: reqwest::Client,
    pub pin: usize,
    pub line_id: u64,
    pub line_name: Option<String>,
    pub db_id: u64,
    pub obj_ref: i64,
    pub status: i64,
    pub register_id: i64,
    pub contact_id: u64,
    pub unique_token: Option<String>,
    pub udp_ka_ack: String,
    pub need_refresh: bool,
    pub last_refresh_time: Option<Instant>,
    pub connected_alarms: AHashSet<i64>,
    pub active_ids: AHashSet<u64>,
    pub active_tags: AHashSet<u64>,
    pub new_pid: u32,
}

impl DCareDevice {
    pub async fn new(server_addr: &str, pin: usize) -> Result<Self, anyhow::Error> {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.connect(server_addr).await?;
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?;

        Ok(Self {
            socket,
            http_client,
            pin,
            unique_token: None,
            line_id: 0,
            line_name: None,
            db_id: 0,
            register_id: 0,
            obj_ref: 0,
            status: 0,
            contact_id: 0,
            udp_ka_ack: Self::get_udp_ka_ack(pin),
            need_refresh: true,
            last_refresh_time: None,
            connected_alarms: AHashSet::new(),
            active_ids: AHashSet::new(),
            active_tags: AHashSet::new(),
            new_pid: 0,
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

    #[inline]
    fn get_device_id(pin: usize) -> String {
        format!("__SIM_DCARE_{pin:03}")
    }

    #[inline]
    fn get_device_name(pin: usize) -> String {
        format!("DcareEX Sim {pin:03}")
    }

    #[inline]
    fn get_udp_register(&self) -> String {
        json!({
            "Type": "Register",
            "DeviceId": Self::get_device_id(self.pin),
            "ObjectType": self.unique_token,
        })
        .to_string()
    }

    #[inline]
    fn get_udp_ka_ack(pin: usize) -> String {
        json!({
            "Type": "Ack",
            "DeviceId": Self::get_device_id(pin),
        })
        .to_string()
    }

    pub async fn initialize(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
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
        self.socket.send(self.get_udp_register().as_bytes()).await?;
        Ok(())
    }

    pub async fn deinitialize(&mut self, server_addr: &str, token_pid: &str) {
        if self.register_id != 0
            && let Err(err) = self
                .request_bulk_connect(server_addr, token_pid, false)
                .await
        {
            log!(
                0,
                "[DCare_{:03}] Failed to disconnect bulk from server: {err}",
                self.pin
            );
        }
        if self.contact_id > 0
            && let Err(err) = self.request_sign_out(server_addr, token_pid).await
        {
            log!(
                0,
                "[DCare_{:03}] Failed to sign out contact: {err}",
                self.pin
            );
        }
        if self.status == 1
            && let Err(err) = self.request_stop_device(server_addr, token_pid).await
        {
            log!(0, "[DCare_{:03}] Failed to stop device: {err}", self.pin);
        }
    }

    pub async fn check_interval(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        if self.need_refresh
            || self
                .last_refresh_time
                .is_some_and(|t| t.elapsed().as_secs() > 30)
        {
            if !self.need_refresh && self.register_id == 0 {
                log!(
                    0,
                    "[DCare_{:03}] UDP Register not working. Pleaase check server is reachable on UDP at {}",
                    self.pin,
                    server_addr,
                );
            }
            let instant = Instant::now();
            self.need_refresh = false;
            self.last_refresh_time = Some(instant);
            self.request_refresh_alarm_list(server_addr, token_pid)
                .await?;
            let elapsed = instant.elapsed().as_millis();
            let level = match elapsed {
                0..200 => 4,
                200..500 => 3,
                500..1000 => 2,
                _ => 1,
            };
            log!(
                level,
                "[DCare_{:03}] Refreshing Alarm List took: {} ms",
                self.pin,
                elapsed
            );
        }
        Ok(())
    }

    pub async fn process_recv_udp(
        &mut self,
        server_addr: &str,
        token_pid: &str,
        data: &[u8],
    ) -> Result<(), anyhow::Error> {
        let json_val: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| anyhow::anyhow!("Failed to parse data as JSON: {e}"))?;
        if json_val.get("KeepAlive").is_some() {
            log!(4, "[DCare_{:03}] KeepAlive received", self.pin);
            self.socket.send(self.udp_ka_ack.as_bytes()).await?;
        } else {
            if let Some(registered) = json_val
                .get("Registered")
                .map(|v| v.is_boolean() && v.as_bool().unwrap_or(false))
            {
                if !registered {
                    return Err(anyhow::anyhow!("Device registered: false"));
                }
                self.register_id = json_val
                    .get("Object")
                    .and_then(|v| v.as_str()?.parse::<i64>().ok())
                    .unwrap_or(0);
                if self.register_id == 0 {
                    return Err(anyhow::anyhow!(
                        "Device registration success but invalid Object: {:?}",
                        json_val.get("Object")
                    ));
                }
                log!(
                    3,
                    "[DCare_{:03}] Registered OK -> register_id: {}",
                    self.pin,
                    self.register_id
                );
                self.request_bulk_connect(server_addr, token_pid, true)
                    .await?;
            } else if let Some(ev_type) = json_val.get("EventType").and_then(|v| v.as_str()) {
                let notifier = json_val
                    .get("Notifier")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                match ev_type {
                    "AlarmAdded" | "AlarmRemoved" => {
                        self.need_refresh = true;
                        let alarm_obj_ref = json_val
                            .get("Alarm")
                            .and_then(|v| v.as_i64())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "N:{notifier} {ev_type} missing 'Alarm' in message: {json_val}"
                                )
                            })?;
                        let alarm_id =
                            json_val.get("Id").and_then(|v| v.as_i64()).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "N:{notifier} {ev_type} missing 'Id' in message: {json_val}"
                                )
                            })?;
                        log!(
                            3,
                            "[DCare_{:03}] N:{notifier} {ev_type}:{alarm_obj_ref} -> Id:{alarm_id}",
                            self.pin
                        );
                        if ev_type == "AlarmAdded" {
                            if !self.connected_alarms.contains(&alarm_obj_ref) {
                                self.request_bulk_connect_alarm_state_changed(
                                    server_addr,
                                    token_pid,
                                    alarm_obj_ref,
                                    true,
                                )
                                .await?;
                                self.connected_alarms.insert(alarm_obj_ref);
                            }
                        } else if self.connected_alarms.remove(&alarm_obj_ref) {
                            self.request_bulk_connect_alarm_state_changed(
                                server_addr,
                                token_pid,
                                alarm_obj_ref,
                                false,
                            )
                            .await?;
                        }
                    }
                    "StateChanged" | "AlarmReceived" => {
                        self.need_refresh = true;
                        log!(
                            4,
                            "[DCare_{:03}] N:{notifier} {ev_type} -> {json_val}",
                            self.pin
                        );
                    }
                    _ => {
                        log!(
                            4,
                            "[DCare_{:03}] N:{notifier} {ev_type} -> {json_val}",
                            self.pin
                        );
                    }
                }
            } else if json_val.get("Activity").is_some() {
                let title = json_val
                    .get("Title")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let tag = json_val
                    .get("Tag")
                    .and_then(|v| v.as_u64())
                    .unwrap_or_default();
                let not_deleted = json_val
                    .get("Delete")
                    .and_then(|v| v.as_bool())
                    .is_some_and(|b| !b);

                if let Some(body) = json_val.get("Body").and_then(|v| v.as_str()) {
                    log!(
                        3,
                        "[DCare_{:03}] Activity Notify: {title} [{body}]",
                        self.pin
                    );
                } else if tag > 0 {
                    let log = if not_deleted {
                        self.active_tags.insert(tag)
                    } else {
                        self.active_tags.remove(&tag)
                    };
                    if log {
                        log!(
                            3,
                            "[DCare_{:03}] Alarm Add:{not_deleted} DisplayNotify: {title} Tag:{tag}",
                            self.pin
                        );
                    }
                } else {
                    log!(
                        4,
                        "[DCare_{:03}] Activity Notify: {title} Tag:{tag} Active:{not_deleted}",
                        self.pin
                    );
                }
            } else {
                log!(
                    4,
                    "[DCare_{:03}] Unknown json received: {json_val}",
                    self.pin
                );
            }
            if let Some(seq_no) = json_val.get("SeqNo").and_then(|v| v.as_u64()) {
                log!(4, "[DCare_{:03}] Ack SeqNo: {seq_no}", self.pin);
                let ack_body = json!({"Type": "Ack", "SeqNo": seq_no}).to_string();
                self.socket.send(ack_body.as_bytes()).await?;
            }
        }
        Ok(())
    }

    async fn request_sign_in(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        match self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC.Contacts"),
                "Login",
                format!(r#"{{"IdEndpoint":{},"PIN":"{}"}}"#, self.db_id, self.pin),
            )
            .await?
        {
            PoltysResponse::Err(poltys_response_error) => {
                Err(anyhow::anyhow!("login failed: {poltys_response_error}"))
            }
            PoltysResponse::Other(val) => {
                self.contact_id = val.as_u64().unwrap_or_else(|| {
                    log!(
                        2,
                        "[DCare_{0:03}] Contact Login response is not a valid number: {val}",
                        self.pin
                    );
                    0
                });
                log!(
                    3,
                    "[DCare_{:03}] Contact Login OK -> contact_id: {:04}",
                    self.pin,
                    self.contact_id
                );
                Ok(())
            }
            o => Err(anyhow::anyhow!(
                "Contact Login unexpected response type {o:?}"
            )),
        }
    }
    async fn request_sign_out(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC.Contacts"),
                "Logout",
                format!(r#"{{"IdEndpoint":{}}}"#, self.db_id),
            )
            .await?
        {
            Err(anyhow::anyhow!("login failed: {poltys_response_error}"))
        } else {
            log!(3, "[DCare_{0:03}] Contact Logout OK", self.pin);
            Ok(())
        }
    }

    async fn request_contact_id(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        let parse_res: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("DCC.Contacts"),
                "IsLoggedIn",
                format!(r#"{{"IdEndpoint":{}}}"#, self.db_id),
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
                anyhow::anyhow!(
                    "IsLoggedIn contact id is not a valid number in response: {parse_res}"
                )
            })?;
        log!(
            3,
            "[DCare_{0:03}] IsLoggedIn OK -> contact id: {1:04}",
            self.pin,
            self.contact_id
        );
        Ok(())
    }

    async fn request_start_device(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.obj_ref),
                "StartScript",
                "{}".to_string(),
            )
            .await?
        {
            Err(anyhow::anyhow!(
                "StartScript failed: {poltys_response_error}"
            ))
        } else {
            log!(3, "[DCare_{0:03}] StartScript OK", self.pin);
            Ok(())
        }
    }

    async fn request_stop_device(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        if let PoltysResponse::Err(poltys_response_error) = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.obj_ref),
                "StopScript",
                "{}".to_string(),
            )
            .await?
        {
            Err(anyhow::anyhow!(
                "StopScript failed: {poltys_response_error}"
            ))
        } else {
            self.status = 0;
            log!(3, "[DCare_{0:03}] StopScript OK", self.pin);
            Ok(())
        }
    }

    async fn request_device_or_new(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        let device_id = Self::get_device_id(self.pin);
        let device_name = Self::get_device_name(self.pin);
        let parse_res: PoltysResponse = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("Devices.Endpoints"),
                "GetDetails",
                format!(
                    r#"{{"HWIdentifier":"{device_id}","Line":"{}","GetData":true,"GetRef":true}}"#,
                    self.line_name.as_ref().unwrap()
                ),
            )
            .await?;
        let dev = match parse_res {
            PoltysResponse::Err(poltys_response_error) => {
                if poltys_response_error.has_part_id("ERR_FIND") {
                    log!(
                        2,
                        "[DCare_{0:03}] device not found, creating new one",
                        self.pin
                    );
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
                            return Err(anyhow::anyhow!(
                                "create device failed: {poltys_response_error}"
                            ));
                        }
                        PoltysResponse::Other(dev) => {
                            log!(
                                1,
                                "[DCare_{0:03}] device created successfully. waiting for it to be ready",
                                self.pin
                            );
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
                    return Err(anyhow::anyhow!(
                        "get details failed: {poltys_response_error}"
                    ));
                }
            }
            PoltysResponse::Other(dev) => {
                log!(
                    4,
                    "[DCare_{0:03}] device already exists, skip creation",
                    self.pin
                );
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
        self.status = dev
            .as_object()
            .and_then(|obj| obj.get("Status"))
            .and_then(|v| v.as_number())
            .ok_or_else(|| anyhow::anyhow!("get details status not found in response: {dev}"))?
            .as_i64()
            .ok_or_else(|| {
                anyhow::anyhow!("get details status is not a valid number in response: {dev}")
            })?;
        let settings = dev
            .get("Settings")
            .and_then(|s| {
                s.as_str()
                    .map(|s| s.to_string())
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s.as_str()).ok())
            })
            .unwrap_or_default();
        self.unique_token = settings
            .get("CloudId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        log!(
            3,
            "[DCare_{0:03}] GetDetails OK -> DB ID: {1}, ObjectRef: {2} Status: {3} ClientID:{4:?}",
            self.pin,
            self.db_id,
            self.obj_ref,
            self.status,
            self.unique_token
        );
        Ok(())
    }

    async fn request_dcare_line_name(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        let parse_res: serde_json::Value = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::Type("Devices.Lines"),
                "GetDetails",
                format!(r#"{{"Id":{0},"GetData":true,"GetRef":true}}"#, self.line_id),
            )
            .await?;
        self.line_name = Some(
            parse_res
                .as_object()
                .and_then(|obj| obj.get("Name"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "[DCare_{0:03}] line name not found in response: {parse_res}",
                        self.pin
                    )
                })?
                .to_string(),
        );
        log!(
            3,
            "[DCare_{0:03}] Line GetDetails OK -> Name: {1}",
            self.pin,
            self.line_name.as_ref().unwrap()
        );
        Ok(())
    }

    async fn request_dcare_line_id(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
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
        log!(
            3,
            "[DCare_{0:03}] Lines List DCare OK -> Line ID: {1}",
            self.pin,
            self.line_id
        );
        Ok(())
    }

    fn create_bulk_connect_body(&self, connect: bool) -> serde_json::Value {
        let src_ep_filter = format!(r#"{{"SrcEndpoint":{}}}"#, self.db_id);
        json!([
            {"Cmd::ConnectionInfo":{"ObjectRef":self.obj_ref,"EventType":"AlarmAdded","Userdata":"Active","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectRef":self.obj_ref,"EventType":"AlarmRemoved","Userdata":"Active","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectRef":self.obj_ref,"EventType":"AlarmReceived","Userdata":"Active","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectRef":self.obj_ref,"EventType":"MutedAlarmChanged","Userdata":"Active","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectType":"Routing","ObjectName":"Activities","EventType":"Created","Filter":src_ep_filter,"Userdata":"Assist","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectType":"Routing","ObjectName":"Activities","EventType":"StateChanged","Filter":src_ep_filter,"Userdata":"Assist","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectType":"Routing","ObjectName":"Activities","EventType":"Closing","Filter":src_ep_filter,"Userdata":"Assist","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectType":"Devices","ObjectName":"Endpoints","EventType":"EndpointChanged","Userdata":"Active","Connect":connect}},
            {"Cmd::ConnectionInfo":{"ObjectType":"DCC","ObjectName":"Contacts","EventType":"ChangedContact","Userdata":"Active","Connect":connect}},
        ])
    }

    fn create_bulk_connect_alarm_state_changed_body(
        &self,
        alarm_id: i64,
        connect: bool,
    ) -> serde_json::Value {
        json!([{"Cmd::ConnectionInfo":{"ObjectRef":alarm_id,"EventType":"StateChanged","Userdata":"Active","Connect":connect}}])
    }

    async fn request_bulk_connect_alarm_state_changed(
        &mut self,
        server_addr: &str,
        token_pid: &str,
        alarm_obj_ref: i64,
        connect: bool,
    ) -> Result<(), anyhow::Error> {
        let body = self.create_bulk_connect_alarm_state_changed_body(alarm_obj_ref, connect);
        let response: PoltysResponse = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.register_id),
                "BulkConnect",
                serde_json::to_string(&body)?,
            )
            .await?;
        match response {
            PoltysResponse::Err(poltys_response_error) => {
                if poltys_response_error.error == "ERR_BAD_PROCESS_ID" {
                    self.new_pid = poltys_response_error.code;
                }
                Err(anyhow::anyhow!(
                    "BulkConnect Alarm {alarm_obj_ref} failed: {poltys_response_error}"
                ))
            }
            PoltysResponse::Other(o) => {
                log!(
                    4,
                    "[DCare_{:03}] BulkConnect Alarm {alarm_obj_ref} OK -> {o}",
                    self.pin
                );
                Ok(())
            }
            _ => Err(anyhow::anyhow!(
                "BulkConnect Alarm {alarm_obj_ref} Unexpected response type: {:?}",
                response
            )),
        }
    }

    async fn request_bulk_connect(
        &self,
        server_addr: &str,
        token_pid: &str,
        connect: bool,
    ) -> Result<(), anyhow::Error> {
        let body = self.create_bulk_connect_body(connect);
        let response: PoltysResponse = self
            .post_request(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.register_id),
                "BulkConnect",
                serde_json::to_string(&body)?,
            )
            .await?;
        match response {
            PoltysResponse::Err(poltys_response_error) => Err(anyhow::anyhow!(
                "BulkConnect failed: {poltys_response_error}"
            )),
            PoltysResponse::Other(o) => {
                if o.as_array().is_none_or(|arr| arr.is_empty()) {
                    log!(1, "[DCare_{:03}] BulkConnect OK but no notifiers", self.pin);
                } else if o
                    .as_array()
                    .unwrap()
                    .iter()
                    .contains(&serde_json::Value::Number(0.into()))
                {
                    log!(
                        1,
                        "[DCare_{:03}] BulkConnect OK but 0 notifiers received",
                        self.pin
                    );
                } else {
                    log!(4, "[DCare_{:03}] BulkConnect OK -> {o}", self.pin);
                }
                Ok(())
            }
            _ => Err(anyhow::anyhow!(
                "BulkConnect Unexpected response type: {:?}",
                response
            )),
        }
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
        let settings = json!({
         "AppName": "DCare",
         "CloudId":  rand::rng().sample_iter(&Alphanumeric).take(64).map(char::from).collect::<String>(),
         "Platform": "android",
         "GenericPush": true,
         "PlatformVer": "99",
         "SoftwareVer": 999}
        );
        json!({
            "New": {
                "Devices::Endpoint": {
                    "Identifier": device_name,
                    "HWIdentifier": device_id,
                    "Description": "Simulated DCare device",
                    "Line": { "Devices::Line": {"Name": line_name} },
                    "EndpointType": { "Devices::EndpointType": {"Name": "DCareDevice", "IsDestination": true} },
                    "EndpointAlarm": ep_alarms,
                    "Settings": settings.to_string(),
                }
            }
        })
    }

    fn create_request_get_chats_body(&self) -> serde_json::Value {
        let a_cond: String = self
            .active_ids
            .iter()
            .map(|aid| {
                format!(
                    "({} = {} AND {})",
                    "{{A}}ActivityEvents.fkeyId_Activity",
                    aid,
                    "{{A}}ActivityEvents.StartDate > '2000-01-01T12:00:00'"
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        json!({"Active":true,"Start":0,"Length":1000,
            "Condition":format!("{} AND ({a_cond})", "(JSON_EXTRACT({{A}}ActivityEvents.Data, '$.chat') IS NOT NULL OR JSON_EXTRACT({{A}}ActivityEvents.Data, '$.audio') IS NOT NULL)"),
            "Columns":["COUNT({{A}}ActivityEvents.fkeyId_Activity), ANY_VALUE({{A}}Activities.Id)"],
            "Join":"LEFT JOIN {{A}}ActivityEvents ON {{A}}ActivityEvents.fkeyId_Activity = {{A}}Activities.Id ",
            "GroupBy":"{{A}}ActivityEvents.fkeyId_Activity"
        })
    }

    async fn request_refresh_alarm_list(
        &mut self,
        server_addr: &str,
        token_pid: &str,
    ) -> Result<(), anyhow::Error> {
        let video_sources: PoltysResponse = self.post_request(
            server_addr,
            token_pid,
            ObjTypeOrRef::Type("Devices::Endpoints"), 
            "ListBySkill", 
            r#"{"SkillType":"LOCATION","Condition":"EndpointTypes.Name = 'VideoSource'","Skill":{"ancestorOrigins":{},"href":"https://localhost/menu/activealarms","origin":"https://localhost","protocol":"https:","host":"localhost","hostname":"localhost","port":"","pathname":"/menu/activealarms","search":"","hash":""}}"#.to_string()
        ).await?;
        if let PoltysResponse::Err(poltys_response_error) = &video_sources {
            if poltys_response_error.error == "ERR_BAD_PROCESS_ID" {
                self.new_pid = poltys_response_error.code;
            }
            return Err(anyhow::anyhow!(
                "ListBySkill VideoSource failed: {poltys_response_error}"
            ));
        }
        log!(
            4,
            "[DCare_{:03}] AlarmRefresh: ListBySkill Video OK -> {video_sources}",
            self.pin
        );
        let alarm_list = self
            .post_request::<serde_json::Value>(
                server_addr,
                token_pid,
                ObjTypeOrRef::ObjectRef(self.obj_ref),
                "ActiveAlarmList",
                r#"{"AcceptedByMe":true,"WithEvents":true}"#.to_string(),
            )
            .await?;
        self.active_ids = AHashSet::from_iter(
            alarm_list
                .as_array()
                .map(|alarms| {
                    alarms
                        .iter()
                        .filter_map(|alarm| {
                            alarm
                                .get("Routing::Activity")
                                .and_then(|v| v.get("Id"))
                                .and_then(|v| v.as_u64())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        );
        log!(
            4,
            "[DCare_{:03}] AlarmRefresh: ActiveAlarmList OK -> {:?}",
            self.pin,
            self.active_ids
        );

        if !self.active_ids.is_empty() {
            let active_chats = self
                .post_request::<serde_json::Value>(
                    server_addr,
                    token_pid,
                    ObjTypeOrRef::Type("Routing::Activities"),
                    "List",
                    self.create_request_get_chats_body().to_string(),
                )
                .await?;
            log!(
                4,
                "[DCare_{:03}] AlarmRefresh: GetNewChatsCount OK -> {active_chats}",
                self.pin
            );
            let active_details = self
                .post_request::<serde_json::Value>(
                    server_addr,
                    token_pid,
                    ObjTypeOrRef::ObjectRef(self.obj_ref),
                    "ActiveAlarmListDetails",
                    format!(
                        r#"{{"Condition": "AActivities.Id IN ({})"}}"#,
                        self.active_ids.iter().map(|id| id.to_string()).join(", ")
                    ),
                )
                .await?;
            log!(
                4,
                "[DCare_{:03}] AlarmRefresh: ActiveAlarmListDetails OK -> {active_details}",
                self.pin
            );
            let muted_alarms = self
                .post_request::<serde_json::Value>(
                    server_addr,
                    token_pid,
                    ObjTypeOrRef::ObjectRef(self.obj_ref),
                    "GetMutedAlarms",
                    "{}".to_string(),
                )
                .await?;
            log!(
                4,
                "[DCare_{:03}] AlarmRefresh: GetMutedAlarms OK -> {muted_alarms}",
                self.pin
            );
        }

        Ok(())
    }
}
