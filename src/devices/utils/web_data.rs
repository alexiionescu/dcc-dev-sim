use chrono::{DateTime, Utc};
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};

use crate::log;

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoltysConnectReq<'a> {
    email: &'a str,
    #[serde(rename = "MD5Password")]
    md5_password: String,
    product: &'static str,
}

impl<'a> PoltysConnectReq<'a> {
    pub fn connect_dcc(user: &'a str, password: &str) -> Self {
        Self {
            email: user,
            md5_password: format!("{:x}", md5::compute(password)),
            product: "DCC",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged, rename_all = "PascalCase")]
pub enum PoltysResponse {
    Err(PoltysResponseError),
    Connect(PoltysConnectRes),
    Login(PoltysLoginRes),
    GetPid(ProcessInfo),
    Other(serde_json::Value),
}

impl std::fmt::Display for PoltysResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoltysResponse::Err(e) => write!(f, "{}", e),
            PoltysResponse::Connect(c) => write!(f, "Connect: {:?}", c),
            PoltysResponse::Login(l) => write!(f, "Login: {:?}", l),
            PoltysResponse::GetPid(p) => write!(f, "GetPid: {:?}", p),
            PoltysResponse::Other(v) => write!(f, "Other: {}", v),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponsePartData {
    pub message_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponsePart {
    #[serde(rename = "Cmd::ResponsePart")]
    pub part: ResponsePartData,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoltysResponseError {
    #[serde(default)]
    pub reponse_parts: Vec<ResponsePart>,
    pub error: String,
    pub error_details: String,
    pub code: u32,
}

impl PoltysResponseError {
    pub fn has_part_id(&self, part_id: &str) -> bool {
        self.reponse_parts
            .iter()
            .any(|p| p.part.message_id == part_id)
    }
}
impl std::fmt::Display for PoltysResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.reponse_parts.is_empty() {
            return write!(
                f,
                "ERR{:03} {} {}",
                self.code, self.error, self.error_details
            );
        }
        let parts: Vec<&str> = self
            .reponse_parts
            .iter()
            .map(|p| p.part.message_id.as_str())
            .collect();
        let parts_str = parts.join(", ");
        write!(f, "{}", parts_str)
    }
}

impl std::error::Error for PoltysResponseError {}

#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LicenseData {
    pub key: String,
    pub is_owned: bool,
    pub is_expired: bool,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct License {
    #[serde(rename = "Admin::LicensesKey")]
    pub data: LicenseData,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoltysConnectRes {
    pub token: String,
    pub name: String,
    pub token_global: String,
    pub licenses: Vec<License>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct PoltysLoginRes {
    pub token: String,
    #[serde(rename = "AddressSSL")]
    pub address: String,
    #[serde(rename = "ServerGUID")]
    pub server_guid: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct PoltysLoginCache {
    pub token: String,
    pub address: String,
    pub time: DateTime<Utc>,
    pub refresh_token: String,
    pub global_token: String,
}

impl PoltysLoginCache {
    pub fn new(conn_res: PoltysConnectRes, login_res: PoltysLoginRes) -> Self {
        Self {
            refresh_token: conn_res.token,
            global_token: conn_res.token_global,
            token: login_res.token,
            address: login_res.address,
            time: Utc::now(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ProcessInfo {
    #[serde(rename = "PID")]
    pub pid: u32,
}

pub enum ObjTypeOrRef {
    ObjectRef(i64),
    Type(&'static str),
}

pub async fn post_request<T>(
    client: Option<&reqwest::Client>,
    server_addr: &str,
    token_pid: &str,
    obj: ObjTypeOrRef,
    method: &str,
    body: String,
) -> Result<T, anyhow::Error>
where
    T: serde::de::DeserializeOwned + std::fmt::Debug,
{
    let owned_client;
    let http_client = match client {
        Some(c) => c,
        None => {
            owned_client = reqwest::Client::new();
            &owned_client
        }
    };
    let obj = match obj {
        ObjTypeOrRef::ObjectRef(ref_id) => format!("oref={ref_id}"),
        ObjTypeOrRef::Type(otype) => format!("otype={otype}"),
    };
    log!(5, "post_request body: {body}");
    let req = http_client
        .post(format!(
            "https://{server_addr}/api.pts?{obj}&method={method}{token_pid}"
        ))
        .header(CONTENT_TYPE, "application/json")
        .body(body);
    log!(5, "DCareDevice::post_request request: {:?}", req);
    let res = req.send().await?;
    let res_json = res.text().await?;
    log!(5, "DCareDevice::post_request response: {res_json}");
    Ok(serde_json::from_str::<T>(&res_json)?)
}
