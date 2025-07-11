use core::str;

use crate::log;
use serde::Serialize;
use serde_json::json;

use super::web_data::*;

pub async fn poltys_connect(
    admin: &str,
    user: &str,
    password: &str,
) -> anyhow::Result<PoltysConnectRes> {
    let body = serde_json::to_string(&PoltysConnectReq::connect_dcc(user, password))?;
    log!(4, "web_login::poltys_connect request {body}");
    match post_request(
        None,
        admin,
        "&token=null",
        ObjTypeOrRef::Type("Admin.Users"),
        "PoltysConnect",
        body,
    )
    .await?
    {
        PoltysResponse::Err(poltys_response_error) => Err(poltys_response_error.into()),
        PoltysResponse::Connect(poltys_connect_res) => Ok(poltys_connect_res),
        o => Err(anyhow::anyhow!(
            "poltys_connect Unexpected response type: {o:?}"
        )),
    }
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoltysLoginReq<'a> {
    pub token: &'a str,
    pub key: &'a str,
}

impl<'a> PoltysLoginReq<'a> {
    pub fn new(token: &'a str, key: &'a str) -> Self {
        Self { token, key }
    }
}

pub async fn login(
    admin: &str,
    conn_res: &PoltysConnectRes,
    server: &str,
) -> anyhow::Result<PoltysLoginRes> {
    let lic = conn_res
        .licenses
        .iter()
        .find(|l| server == l.data.name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "web_login::login Server {} not found in licenses: {:?}",
                server,
                conn_res.licenses
            )
        })?;

    let body = serde_json::to_string(&PoltysLoginReq::new(&conn_res.token, &lic.data.key))?;
    log!(4, "web_login::login request {body}");

    match post_request(
        None,
        admin,
        &format!("&token={}", conn_res.token_global),
        ObjTypeOrRef::Type("Admin.MainServer"),
        "ChooseLicense",
        body,
    )
    .await?
    {
        PoltysResponse::Err(poltys_response_error) => Err(anyhow::anyhow!(
            "web_login::login Unexpected response {poltys_response_error}"
        )),
        PoltysResponse::Login(poltys_login_res) => Ok(poltys_login_res),
        o => Err(anyhow::anyhow!(
            "web_login::login Unexpected response type: {o:?}"
        )),
    }
}

pub async fn renew_token(admin: &str, mut login_cache: PoltysLoginCache) -> PoltysLoginCache {
    let body = json!({
        "AccessToken": login_cache.token,
        "RefreshToken": login_cache.refresh_token,
    });
    log!(4, "web_login::refresh_token request {body}");

    match post_request::<serde_json::Value>(
        None,
        admin,
        "",
        ObjTypeOrRef::Type("Admin.MainServer"),
        "RenewToken",
        body.to_string(),
    )
    .await
    {
        Ok(res) => {
            log!(4, "web_login::renew_token OK -> {res}");
            if let Some(token) = res.get("Token").and_then(|v| v.as_str()) {
                login_cache.token = token.to_string();
                login_cache.time = chrono::Utc::now();
            } else {
                log!(1, "web_login::renew_token No refresh token in response");
            }
        }
        Err(e) => {
            log!(1, "web_login::renew_token Error -> {e}");
        }
    }
    login_cache
}

pub async fn get_pid(server_addr: &str, token: &str) -> anyhow::Result<u32> {
    match post_request(
        None,
        server_addr,
        format!("&token={token}").as_str(),
        ObjTypeOrRef::Type("Utils.Miscellaneous"),
        "GetProcessInfo",
        r#"{}"#.to_string(),
    )
    .await?
    {
        PoltysResponse::Err(poltys_response_error) => {
            if poltys_response_error.error == "ERR_BAD_PROCESS_ID" {
                Ok(poltys_response_error.code) // code is the PID in this case
            } else {
                Err(anyhow::anyhow!(
                    "get_pid Unexpected response {poltys_response_error}"
                ))
            }
        }
        PoltysResponse::GetPid(pinfo) => Ok(pinfo.pid),
        o => Err(anyhow::anyhow!("get_pid Unexpected response type: {o:?}")),
    }
}
