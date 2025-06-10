use std::time::Duration;

use crate::{comm_hub::comm_hub_device::CHDevice, log};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;
mod comm_hub_device;

pub(crate) async fn run() -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);
    let server_addr = args
        .server_addr
        .as_ref()
        .ok_or(anyhow::anyhow!(
            "Server address is not specified. Use --server-addr option."
        ))?
        .as_str();

    let r = 0..args.count;
    let step = args.count / args.concurrent_jobs;
    let mut set = JoinSet::new();
    let ct = CancellationToken::new();
    let tasks: Vec<_> = r
        .step_by(step)
        .map(|start| start..(start + step))
        .map(|r| {
            let ct = ct.clone();
            set.spawn(async move { run_dev_range(ct, server_addr, r).await })
        })
        .collect();

    loop {
        tokio::select! {
            Some(res) = set.join_next() => {
                if let Err(e) = res {
                    log!(1, "Error in device task: {}", e);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nCtrl-C received, shutting down\n");
                break;
            }
        }
    }
    ct.cancel();
    if let Err(err) = timeout(Duration::from_secs(10), set.join_all()).await {
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

    Ok(())
}

async fn run_dev_range(
    ct: CancellationToken,
    server_addr: &str,
    r: std::ops::Range<usize>,
) -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);
    const MAX_DATAGRAM_SIZE: usize = 65_507;
    let thread_id = std::thread::current().id();

    if r.start > 0 {
        tokio::time::sleep(Duration::from_millis(200 * r.start as u64)).await;
    }
    log!(1, "[Th:{thread_id:?}] STARTED {r:?}");

    let mut devices = Vec::with_capacity(r.len());
    let tstamp = Instant::now();
    for idx in r {
        let pin = args.dev_id_base + idx;
        if let Ok(mut device) = CHDevice::new(server_addr, pin).await {
            match device.initialize().await {
                Ok(_) => {
                    log!(
                        3,
                        "[CH_{pin:03}] Device initialized successfully on {}",
                        device.socket.local_addr()?
                    );
                    devices.push(device);
                }
                Err(e) => log!(1, "[CH_{pin:03}] Initialization failed: {}", e),
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
    let c_interval_minute = std::time::Duration::from_secs(60);
    let mut check_timer_minute =
        tokio::time::interval_at(Instant::now() + c_interval_minute, c_interval_minute);
    let mut data = vec![0u8; MAX_DATAGRAM_SIZE];
    loop {
        tokio::select! {
            _ = check_timer_minute.tick() => {
                    for device in devices.iter_mut() {
                        if let Err(e) =  device.check_interval_minute().await {
                            log!(1, "[CH_{:03}] Error during check_interval: {}", device.pin, e);
                        }
                    }

            }
            _ = async {
                for device in devices.iter_mut() {
                    match device.socket.recv(&mut data).await {
                        Ok(size) => {
                            if size > 0 {
                                if let Err(e) = device.process_recv_udp(&data[..size]).await {
                                    log!(1, "[CH_{:03}] Error processing udp data: {}", device.pin, e);
                                }
                            }
                        }
                        Err(e) => {
                            log!(1, "[CH_{:03}] Error receive udp data: {}", device.pin, e);
                        }
                    }
                }
            } => {}
            _ = ct.cancelled() => {
                break;
            }
        }
    }

    for device in devices.iter_mut() {
        device.deinitialize().await;
    }
    Ok(())
}
