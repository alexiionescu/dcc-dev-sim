use crate::log;
use ahash::AHashSet;
use serde_json::json;
use std::net::Ipv4Addr;

const PIN_CALL_CORD: u8 = 26; // Pin for Call Cord and Primary Plubger, used in CHDevice
const PIN_RESET: u8 = 17; // Pin reset

pub struct CHDevice {
    pub socket: tokio::net::UdpSocket,
    pub pin: usize,
    seq: u64,
    db_id: Option<u64>,
    waited_ack: AHashSet<u64>,
    has_alarm: u8,
}

impl CHDevice {
    pub async fn new(server_addr: &str, pin: usize) -> Result<Self, anyhow::Error> {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.connect(server_addr).await?;
        Ok(Self {
            socket,
            pin,
            seq: 0,
            db_id: None,
            waited_ack: AHashSet::new(),
            has_alarm: 0,
        })
    }

    #[inline]
    fn get_device_id(&self) -> String {
        format!("__SIM_CH_{:03}", self.pin)
    }

    #[inline]
    fn get_next_seq(&mut self, wait_ack: bool) -> u64 {
        self.seq += 1;
        if wait_ack {
            self.waited_ack.insert(self.seq);
        }
        self.seq
    }

    #[inline]
    fn get_config(&mut self) -> serde_json::Value {
        json!({
            "CMD": "GetConfig",
            "DATA": {
                "Version": "CHSIM_1.0"
            },
            "DID": self.get_device_id(),
            "SEQ": self.get_next_seq(true),
        })
    }

    #[inline]
    fn get_keepalive(&mut self) -> serde_json::Value {
        let mut ka = json!({
            "CMD": "KeepAlive",
            "DID": self.get_device_id(),
            "SEQ": self.get_next_seq(false),
        });
        if let Some(db_id) = self.db_id {
            ka.as_object_mut().unwrap().insert(
                "DCC_ID".to_string(),
                serde_json::Value::Number(serde_json::Number::from(db_id)),
            );
        };
        ka
    }

    #[inline]
    fn get_send_pin(
        &mut self,
        pin: u8,
        edge: Option<u8>,
        tamper: Option<u8>,
        cord_unplugged: Option<u8>,
    ) -> serde_json::Value {
        let mut pin_data = json!({
            "Id": pin,
        });
        if let Some(edge) = edge {
            pin_data.as_object_mut().unwrap().insert(
                "Edge".to_string(),
                serde_json::Value::Number(serde_json::Number::from(edge)),
            );
        }
        if let Some(tamper) = tamper {
            pin_data
                .as_object_mut()
                .unwrap()
                .insert("Tamper".to_string(), tamper.into());
        }
        if let Some(cord_unplugged) = cord_unplugged {
            pin_data
                .as_object_mut()
                .unwrap()
                .insert("TamperCord".to_string(), cord_unplugged.into());
        }
        let mut pin_cmd = json!({
            "CMD": "Pin",
            "DATA": pin_data,
            "DID": self.get_device_id(),
            "SEQ": self.get_next_seq(true),
        });
        if let Some(db_id) = self.db_id {
            pin_cmd.as_object_mut().unwrap().insert(
                "DCC_ID".to_string(),
                serde_json::Value::Number(serde_json::Number::from(db_id)),
            );
        };
        pin_cmd
    }

    pub async fn initialize(&mut self) -> Result<(), anyhow::Error> {
        let get_cfg = self.get_config();
        self.socket.send(get_cfg.to_string().as_bytes()).await?;
        Ok(())
    }
    pub async fn deinitialize(&mut self) {
        // No specific deinitialization for CHDevice
        let no_ack = self.waited_ack.len();
        log!(
            if no_ack > 0 { 3 } else { 4 },
            "[CH_{:03}] Device deinitialized. Unreceived ACK:{}",
            self.pin,
            self.waited_ack.len()
        );
    }

    pub async fn check_interval_minute(&mut self) -> Result<(), anyhow::Error> {
        if self.has_alarm == 0 && rand::random::<u64>() % (24 * 60) == 0 {
            // 1 alarms per day on average
            self.has_alarm = 1;
            let pin_cmd = self.get_send_pin(PIN_CALL_CORD, None, None, None);
            self.socket.send(pin_cmd.to_string().as_bytes()).await?;
            log!(
                3,
                "[CH_{:03}] Sending Primary Call Cord Alarm {}",
                self.pin,
                self.seq
            );
        } else if self.has_alarm == 5 {
            self.has_alarm = 0;
            let pin_cmd = self.get_send_pin(PIN_RESET, None, None, None);
            self.socket.send(pin_cmd.to_string().as_bytes()).await?;
            log!(3, "[CH_{:03}] Sending Reset {}", self.pin, self.seq);
        } else {
            log!(4, "[CH_{:03}] Sending KeepAlive", self.pin);
            if self.has_alarm > 0 {
                self.has_alarm += 1;
            }
            let ka = self.get_keepalive();
            self.socket.send(ka.to_string().as_bytes()).await?;
        }
        Ok(())
    }

    pub async fn process_recv_udp(&mut self, data: &[u8]) -> Result<(), anyhow::Error> {
        let json_val: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| anyhow::anyhow!("Failed to parse data as JSON: {e}"))?;
        if let Some(notify) = json_val.get("NOTIFY").and_then(|v| v.as_str()) {
            match notify {
                "Start" => {
                    log!(3, "[DCare_{:03}] NOTIFY Start. Send GetConfig", self.pin);
                    let get_cfg = self.get_config();
                    self.socket.send(get_cfg.to_string().as_bytes()).await?;
                }
                _ => log!(4, "[DCare_{:03}] NOTIFY received {notify}", self.pin),
            }
        } else if let Some(ack) = json_val.get("ACK").and_then(|v| v.as_u64()) {
            if self.waited_ack.remove(&ack) {
                log!(3, "[CH_{:03}] ACK received {ack}", self.pin);
                if let Some(data) = json_val.get("DATA").and_then(|v| v.as_object()) {
                    if let Some(db_id) = data.get("DCC_ID").and_then(|v| v.as_u64()) {
                        self.db_id = Some(db_id);
                        log!(3, "[CH_{:03}] DCC_ID set to {db_id}", self.pin);
                    }
                }
            } else {
                log!(2, "[CH_{:03}] Unexpected ACK received: {ack}", self.pin);
            }
        } else {
            log!(
                2,
                "[CH_{:03}] Unknown response received: {json_val}",
                self.pin
            );
        }
        Ok(())
    }
}
