use std::time::Duration;

use super::utils::web_data::PoltysLoginCache;
use chrono::Utc;
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    devices::utils::web_login::{get_pid, login, poltys_connect, renew_token},
    log,
};

mod dcare_device;

use dcare_device::DCareDevice;
const DATA_FOLDER: &str = "data";
const LOGIN_CACHE_FILENAME: &str = "login_cache.json";
const LOGIN_CACHE_PATH: &str = const_str::concat!(DATA_FOLDER, "/", LOGIN_CACHE_FILENAME);
const DELAY: u64 = 200; // milliseconds init/deinit delay between spawning tasks

#[derive(Default, Clone)]
struct WatchData {
    token: Option<String>,
}

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
    let token = login_cache.as_ref().map(|c| &c.token).unwrap().clone();
    let server_addr = args
        .server_addr
        .as_ref()
        .or(login_cache.as_ref().map(|c| &c.address))
        .unwrap()
        .clone();

    let r = 0..args.count;
    let step = args.count / args.concurrent_jobs;
    let mut set = JoinSet::new();
    let ct = CancellationToken::new();
    let (watcher_tx, watcher_rx) = tokio::sync::watch::channel(WatchData::default());

    let tasks: Vec<_> = r
        .step_by(step)
        .map(|start| start..(start + step))
        .map(|r| {
            let token = token.clone();
            let server_addr = server_addr.clone();
            let ct = ct.clone();
            let watcher_rx = watcher_rx.clone();
            set.spawn(async move { run_dev_range(ct, watcher_rx, token, &server_addr, r).await })
        })
        .collect();

    let c_interval_daily = Duration::from_secs(86400);
    let mut check_timer_daily =
        tokio::time::interval_at(Instant::now() + c_interval_daily, c_interval_daily);
    loop {
        tokio::select! {
            Some(res) = set.join_next() => {
                if let Err(e) = res {
                    log!(1, "Error in device task: {}", e);
                }
            }
            _ = check_timer_daily.tick() => {
                login_cache = Some(renew_token(&args.admin, login_cache.take().unwrap()).await);
                watcher_tx.send_if_modified(|data| {
                    if data.token.as_deref() != login_cache.as_ref().map(|c| c.token.as_str()) {
                        log!(3, "[Login] Token changed. Sending to all tasks");
                        data.token = login_cache.as_ref().map(|c| c.token.clone());
                        true
                    }
                    else {false}
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nCtrl-C received, shutting down\n");

                break;
            }
        }
    }
    ct.cancel();
    if let Err(err) = timeout(
        Duration::from_secs(5 + DELAY * args.count as u64),
        set.join_all(),
    )
    .await
    {
        log!(
            0,
            "Timeout while waiting for all tasks to finish gracefully: {}",
            err
        );
        tasks.iter().for_each(|task| {
            task.abort();
        });
        log!(0, "*** All tasks aborted ***");
    } else {
        log!(0, "*** All tasks finished successfully ***");
    }

    tokio::fs::write(
        LOGIN_CACHE_PATH,
        serde_json::to_string(&login_cache.unwrap())?,
    )
    .await?;
    Ok(())
}

async fn run_dev_range(
    ct: CancellationToken,
    mut watcher_rx: watch::Receiver<WatchData>,
    mut token: String,
    server_addr: &str,
    r: std::ops::Range<usize>,
) -> Result<(), anyhow::Error> {
    const MAX_DATAGRAM_SIZE: usize = 65_507;
    let thread_id = std::thread::current().id();

    let delay = tokio::time::Duration::from_millis(DELAY * r.start as u64);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    log!(1, "[{thread_id:?}] STARTED {r:?}");

    let mut pid = get_pid(server_addr, &token).await?;

    let args = &(*crate::ARGS);
    let mut devices = Vec::with_capacity(r.len());
    let mut token_pid = format!("&token={token}&pid={pid}");
    let tstamp = Instant::now();
    for idx in r {
        let pin = args.dev_id_base + idx;
        if let Ok(mut device) = DCareDevice::new(server_addr, pin).await {
            match device.initialize(server_addr, &token_pid).await {
                Ok(_) => {
                    log!(
                        2,
                        "[DCare_{pin:03}] Device initialized successfully on {}",
                        device.socket.local_addr()?
                    );
                    devices.push(device);
                }
                Err(e) => log!(1, "[DCare_{pin:03}] Initialization failed: {}", e),
            }
        }
    }
    log!(
        0,
        "*** {} DCare devices runnning. Init Duration {} ms  ***",
        devices.len(),
        tstamp.elapsed().as_millis()
    );
    if devices.is_empty() {
        return Ok(());
    }
    let c_interval = std::time::Duration::from_millis(500);
    let mut check_timer = tokio::time::interval_at(Instant::now() + c_interval, c_interval);
    let mut data = vec![0u8; MAX_DATAGRAM_SIZE];
    let mut server_restarted = false;
    loop {
        tokio::select! {
            biased;

            _ = ct.cancelled() => {
                break;
            }
            _ = watcher_rx.changed() => {
                let mut watch_data = watcher_rx.borrow_and_update().clone();
                if let Some(new_token) = watch_data.token.take() {
                    token = new_token;
                    log!(3, "[{thread_id:?}] New Token received, updating token_pid");
                    token_pid = format!("&token={token}&pid={pid}");
                }
            }
            _ = async {
                for device in devices.iter_mut() {
                    match device.socket.recv(&mut data).await {
                        Ok(size) => {
                            if size > 0 {
                                if let Err(e) = device.process_recv_udp(server_addr, &token_pid, &data[..size]).await {
                                    log!(1, "[DCare_{:03}] Error processing udp data: {}", device.pin, e);
                                    if e.to_string().contains("ERR_BAD_PROCESS_ID") {
                                        log!(1, "[{thread_id:?}] PID changed from {} to {}, updating token_pid", pid, device.new_pid);
                                        pid = device.new_pid;
                                        token_pid = format!("&token={token}&pid={pid}");
                                        server_restarted = true;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log!(1, "[DCare_{:03}] Error receive udp data: {}", device.pin, e);
                        }
                    }
                }
            } => {}
            _ = check_timer.tick() => {
                if !server_restarted {
                    for device in devices.iter_mut() {
                        if let Err(e) =  device.check_interval(server_addr, &token_pid).await {
                            log!(1, "[DCare_{:03}] Error during check_interval: {}", device.pin, e);
                            if e.to_string().contains("ERR_BAD_PROCESS_ID") {
                                log!(1, "[{thread_id:?}] PID changed from {} to {}, updating token_pid", pid, device.new_pid);
                                pid = device.new_pid;
                                token_pid = format!("&token={token}&pid={pid}");
                                server_restarted = true;
                                break;
                            }
                        }
                    }
                }
                if server_restarted {
                    log!(1, "[{thread_id:?}] Server restarted, reinitialize devices");
                    for device in devices.iter_mut() {
                        if let Err(e) = device.initialize(server_addr, &token_pid).await {
                            log!(1, "[DCare_{:03}] Error during reinitialization: {}", device.pin, e);
                        }
                    }
                    server_restarted = false;
                }
            }
        }
    }

    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    for device in devices.iter_mut() {
        device.deinitialize(server_addr, &token_pid).await;
    }

    Ok(())
}
