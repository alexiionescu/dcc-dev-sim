use core::str;
use std::time::Duration;

use crate::log;
use serde::Serialize;
use serde_json::json;

use super::web_data::*;

async fn poltys_connect(
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

async fn login(
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

pub async fn get_pid(
    server_addr: &str,
    token: &str,
    client: Option<&reqwest::Client>,
    mut connection_timeout: u64,
) -> anyhow::Result<u32> {
    log!(2, "Connecting to server instance ...");
    let token_param = format!("&token={token}");
    let mut tokio_check_timer = tokio::time::interval(Duration::from_secs(1));
    match loop {
        tokio::select! {
            r = post_request(
                client,
                server_addr,
                &token_param,
                ObjTypeOrRef::Type("Utils.Miscellaneous"),
                "GetProcessInfo",
                r#"{}"#.to_string(),
            ) => break r?,
            _ = tokio_check_timer.tick() => {
                connection_timeout -= 1;
                if connection_timeout == 0 {
                    return Err(anyhow::anyhow!("Connection timeout. Please check server {} is reachable on TCP.", server_addr));
                }
                if connection_timeout % 10 == 0 {
                    log!(2, "Connecting to server instance ...{:2}s left", connection_timeout);
                }
            }
        }
    } {
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

const DATA_FOLDER: &str = "data";
const LOGIN_CACHE_FILENAME: &str = "login_cache.json";
const LOGIN_CACHE_PATH: &str = const_str::concat!(DATA_FOLDER, "/", LOGIN_CACHE_FILENAME);

pub async fn login_with_cache() -> anyhow::Result<PoltysLoginCache> {
    let args = &(*crate::ARGS);
    let login_cache_path = args
        .login_cache
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or(LOGIN_CACHE_PATH);

    tokio::fs::create_dir_all(DATA_FOLDER).await?;
    let mut login_cache = if args.no_login_cache {
        None
    } else {
        tokio::fs::read(login_cache_path)
            .await
            .map(|data| serde_json::from_slice::<PoltysLoginCache>(&data).ok())
            .ok()
            .flatten()
    };
    if login_cache
        .as_ref()
        .is_none_or(|c| chrono::Utc::now().signed_duration_since(c.time).num_days() > 1)
    {
        let conn_res = poltys_connect(&args.admin, &args.user, &args.password).await?;
        let login_res = login(&args.admin, &conn_res, &args.server).await?;
        login_cache = Some(PoltysLoginCache::new(conn_res, login_res));
        log!(
            0,
            "[Login] Login OK {}",
            login_cache.as_ref().unwrap().address,
        );
        log!(
            2,
            "[Login] token={} time={}",
            login_cache.as_ref().unwrap().token,
            login_cache.as_ref().unwrap().time
        );
    }
    let login_cache = login_cache.unwrap();
    save_login_cache(&login_cache).await?;
    Ok(login_cache)
}

pub async fn save_login_cache(login_cache: &PoltysLoginCache) -> anyhow::Result<()> {
    let args = &(*crate::ARGS);
    let login_cache_path = args
        .login_cache
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or(LOGIN_CACHE_PATH);
    tokio::fs::write(login_cache_path, serde_json::to_string(login_cache)?).await?;
    Ok(())
}
