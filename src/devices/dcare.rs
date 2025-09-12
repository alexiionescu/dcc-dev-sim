use std::time::Duration;

use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    devices::utils::web_login::{get_pid, login_with_cache, renew_token},
    log,
};

mod dcare_device;

use dcare_device::DCareDevice;
const DELAY: u64 = 200; // milliseconds init/deinit delay between spawning tasks

#[derive(Default, Clone)]
struct WatchData {
    token: Option<String>,
}

pub(crate) async fn run() -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);

    let mut login_cache = login_with_cache().await?;
    let token = login_cache.token.clone();
    let server_addr = args
        .server_addr
        .as_ref()
        .unwrap_or(&login_cache.address.clone())
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
                match res {
                    Err(e) => log!(0, "A device task has panicked: {}", e),
                    Ok(Err(e)) => log!(0, "A device task returned an error: {}", e),
                    Ok(Ok(())) => log!(0, "A device task has finished unexpectedly."),
                }
                if set.is_empty() {
                    log!(0, "All device tasks have finished, exiting main loop.");
                    break;
                }
            }
            _ = check_timer_daily.tick() => {
                login_cache = renew_token(&args.admin, login_cache).await;
                watcher_tx.send_if_modified(|data| {
                    if data.token.as_deref() != Some(login_cache.token.as_str()) {
                        log!(3, "[Login] Token changed. Sending to all tasks");
                        data.token = Some(login_cache.token.clone());
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
        log!(0, "*** All tasks finished ***");
    }

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

    let mut pid = get_pid(server_addr, &token, None, 5).await?;

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
                Err(e) => log!(0, "[DCare_{pin:03}] Initialization failed: {}", e),
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
                            if size > 0
                                && let Err(e) = device.process_recv_udp(server_addr, &token_pid, &data[..size]).await {
                                    log!(1, "[DCare_{:03}] Error processing udp data: {}", device.pin, e);
                                    if e.to_string().contains("ERR_BAD_PROCESS_ID") {
                                        log!(1, "[{thread_id:?}] PID changed from {} to {}, updating token_pid", pid, device.new_pid);
                                        pid = device.new_pid;
                                        token_pid = format!("&token={token}&pid={pid}");
                                        server_restarted = true;
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
