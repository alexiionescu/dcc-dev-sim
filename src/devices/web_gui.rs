use std::time::Duration;

use super::utils::web_data::PoltysLoginCache;
use chrono::Utc;
use tokio::time::Instant;

use crate::{
    ArgsCommand,
    devices::utils::{
        web_data::{ObjTypeOrRef, PoltysResponse, post_request},
        web_login::{get_pid, login, poltys_connect, renew_token},
    },
    log,
};

mod web_browser;

use web_browser::WebClient;
const DATA_FOLDER: &str = "data";
const LOGIN_CACHE_FILENAME: &str = "login_cache.json";
const LOGIN_CACHE_PATH: &str = const_str::concat!(DATA_FOLDER, "/", LOGIN_CACHE_FILENAME);

pub(crate) async fn run() -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);
    let login_cache_path = args
        .login_cache
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or(LOGIN_CACHE_PATH);

    tokio::fs::create_dir_all(DATA_FOLDER).await?;
    let mut login_cache = tokio::fs::read(login_cache_path)
        .await
        .map(|data| serde_json::from_slice::<PoltysLoginCache>(&data).ok())
        .ok()
        .flatten();

    if login_cache
        .as_ref()
        .is_none_or(|c| Utc::now().signed_duration_since(c.time).num_days() > 1)
    {
        let conn_res = poltys_connect(&args.admin, &args.user, &args.password).await?;
        let login_res = login(&args.admin, &conn_res, &args.server).await?;
        login_cache = Some(PoltysLoginCache::new(conn_res, login_res));
        log!(
            0,
            "[Login] Login OK {} token={} time={}",
            login_cache.as_ref().unwrap().address,
            login_cache.as_ref().unwrap().token,
            login_cache.as_ref().unwrap().time
        );
    }
    let mut token = login_cache.as_ref().map(|c| &c.token).unwrap().clone();
    let server_addr = args
        .server_addr
        .as_ref()
        .or(login_cache.as_ref().map(|c| &c.address))
        .unwrap()
        .clone();

    let mut pid = get_pid(&server_addr, &token).await?;

    let c_interval_daily = Duration::from_secs(86400);
    let mut check_timer_daily =
        tokio::time::interval_at(Instant::now() + c_interval_daily, c_interval_daily);
    let r = 0..args.count;
    let mut devices = Vec::with_capacity(r.len());
    let mut token_pid = format!("&token={token}&pid={pid}");
    let tstamp = Instant::now();
    for idx in r {
        let pin = args.dev_id_base + idx;
        if let Ok(mut device) = WebClient::new(pin) {
            match device.initialize(&server_addr).await {
                Ok(_) => {
                    log!(2, "[Web_{pin:03}] Device initialized successfully");
                    devices.push(device);
                }
                Err(e) => log!(1, "[Web_{pin:03}] Initialization failed: {}", e),
            }
        } else {
            log!(1, "[Web_{pin:03}] Failed to create WebClient instance");
        }
    }
    log!(
        0,
        "*** {} Web devices running. Init Duration {} ms  ***",
        devices.len(),
        tstamp.elapsed().as_millis()
    );
    if devices.is_empty() {
        tokio::fs::write(
            login_cache_path,
            serde_json::to_string(&login_cache.unwrap())?,
        )
        .await?;
        log!(0, "No devices to run, exiting.");
        return Ok(());
    }
    let c_interval = std::time::Duration::from_millis(500);
    let mut check_timer = tokio::time::interval_at(Instant::now() + c_interval, c_interval);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nCtrl-C received, shutting down\n");
                break;
            }
            _ = async {
                for device in devices.iter_mut() {
                    if let Some(ws) = &mut device.ws {
                        match ws.read_frame().await {
                            Ok(msg) => {
                                if let Err(e) = device.process_ws_msg(&server_addr, &token_pid, msg).await {
                                    log!(1, "[Web_{:03}] Error processing websocket data: {}", device.pin, e);
                                    if device.new_pid > 0 && pid != device.new_pid {
                                        log!(1, "PID changed from {} to {}, updating token_pid", pid, device.new_pid);
                                        pid = device.new_pid;
                                        token_pid = format!("&token={token}&pid={pid}");
                                    }
                                }
                            }
                            Err(e) => {
                                log!(1, "[Web_{:03}] Error websocket: {}", device.pin, e);
                                device.ws = None; // Reset the websocket connection
                            }
                        }
                    }
                }
            } => {}
            _ = check_timer.tick() => {
                for device in devices.iter_mut() {
                    if let Err(e) =  device.check_interval(&server_addr, &token_pid).await {
                        log!(1, "[Web_{:03}] Error during check_interval: {}", device.pin, e);
                        if device.new_pid > 0 && pid != device.new_pid {
                            log!(1, "PID changed from {} to {}, updating token_pid", pid, device.new_pid);
                            pid = device.new_pid;
                            token_pid = format!("&token={token}&pid={pid}");
                        }
                    }
                }
            }
            _ = check_timer_daily.tick() => {
                login_cache = Some(renew_token(&args.admin, login_cache.take().unwrap()).await);
                token = login_cache.as_ref().map(|c| &c.token).unwrap().clone();
                log!(3, "[Login] Token changed.");
                token_pid = format!("&token={token}&pid={pid}");
            }
        }
    }

    for device in devices.iter_mut() {
        if let Err(e) = device.deinitialize(&server_addr, &token_pid).await {
            log!(
                1,
                "[Web_{:03}] Error during deinitialization: {}",
                device.pin,
                e
            );
        }
    }

    tokio::fs::write(
        login_cache_path,
        serde_json::to_string(&login_cache.unwrap())?,
    )
    .await?;
    Ok(())
}

pub(crate) async fn run_request() -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);
    let (otype, method, months, archive, max_duration) = match &args.cmd {
        ArgsCommand::WebRequest {
            otype,
            method,
            months,
            archive,
            max_duration,
        } => (otype, method, *months, *archive, *max_duration),
        _ => return Err(anyhow::anyhow!("Invalid command for run_request")),
    };
    let login_cache_path = args
        .login_cache
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or(LOGIN_CACHE_PATH);

    tokio::fs::create_dir_all(DATA_FOLDER).await?;
    let mut login_cache = tokio::fs::read(login_cache_path)
        .await
        .map(|data| serde_json::from_slice::<PoltysLoginCache>(&data).ok())
        .ok()
        .flatten();

    if login_cache
        .as_ref()
        .is_none_or(|c| Utc::now().signed_duration_since(c.time).num_days() > 1)
    {
        let conn_res = poltys_connect(&args.admin, &args.user, &args.password).await?;
        let login_res = login(&args.admin, &conn_res, &args.server).await?;
        login_cache = Some(PoltysLoginCache::new(conn_res, login_res));
        log!(
            0,
            "[Login] Login OK {} token={} time={}",
            login_cache.as_ref().unwrap().address,
            login_cache.as_ref().unwrap().token,
            login_cache.as_ref().unwrap().time
        );
    }
    let token = login_cache.as_ref().map(|c| &c.token).unwrap().clone();
    let server_addr = args
        .server_addr
        .as_ref()
        .or(login_cache.as_ref().map(|c| &c.address))
        .unwrap()
        .clone();

    let pid = get_pid(&server_addr, &token).await?;
    let token_pid = format!("&token={token}&pid={pid}");
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(max_duration))
        .build()?;
    let body = match method.as_str() {
        "List" | "Count" => {
            format!(
                r#"{{
                    "Start":0,"Length":1000,"OrderC":[1],"OrderT":["DESC"],
                    "Condition":"((LAST_DAY(@currTimeUTC + INTERVAL 0 MINUTE) + INTERVAL 1 DAY - INTERVAL {months} MONTH - INTERVAL 0 MINUTE <= {{1}} AND {{1}} < LAST_DAY(@currTimeUTC + INTERVAL 0 MINUTE) + INTERVAL 1 DAY - INTERVAL 0 MINUTE))","Having":"",
                    "FromArchive":{archive}}}"#
            )
        }
        _ => "{}".to_string(),
    };
    log!(2, "Exeuting test request...");
    let time = Instant::now();
    let res: PoltysResponse = post_request(
        Some(&http_client),
        &server_addr,
        &token_pid,
        ObjTypeOrRef::Type(otype),
        method,
        body.clone(),
    )
    .await?;
    if let PoltysResponse::Err(poltys_response_error) = &res {
        return Err(anyhow::anyhow!("request failed: {poltys_response_error}"));
    } else {
        log!(
            0,
            "Test request successful. Duration: {} ms",
            time.elapsed().as_millis()
        );
        match res {
            PoltysResponse::Other(data) => {
                log!(3, "Response data: {data:#}");
                if data.is_array() {
                    log!(
                        2,
                        "Response is an array with {} items",
                        data.as_array().unwrap().len()
                    );
                } else if data.is_number() {
                    log!(2, "Response is a number: {}", data.as_number().unwrap());
                }
            }
            _ => log!(2, "Unexpected Test request response: {:?}", res),
        }
        if args.concurrent_jobs > 1 {
            log!(
                2,
                "The request will now run on {} concurrent tasks for {} iterations",
                args.concurrent_jobs,
                args.count
            );

            for i in 0..args.count {
                let mut join_set = tokio::task::JoinSet::new();
                let time = Instant::now();
                for _ in 0..args.concurrent_jobs {
                    let body = body.clone();
                    let http_client = http_client.clone();
                    let server_addr = server_addr.clone();
                    let token_pid = token_pid.clone();
                    join_set.spawn(async move {
                        let _: Result<PoltysResponse, _> = post_request(
                            Some(&http_client),
                            &server_addr,
                            &token_pid,
                            ObjTypeOrRef::Type(otype),
                            method,
                            body,
                        )
                        .await;
                    });
                }
                join_set.join_all().await;
                log!(
                    2,
                    "#{:03}. All requests completed in {} ms",
                    i + 1,
                    time.elapsed().as_millis()
                );
            }
        }
    }

    tokio::fs::write(
        login_cache_path,
        serde_json::to_string(&login_cache.unwrap())?,
    )
    .await?;
    Ok(())
}
