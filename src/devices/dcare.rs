use tokio::time::Instant;
use web_data::PoltysLoginRes;

use crate::log;

mod dcare_device;
pub(crate) mod web_data;

mod web_login;
use dcare_device::DCareDevice;
const DATA_FOLDER: &str = "data";
const LOGIN_CACHE_FILENAME: &str = "login_cache.json";
const LOGIN_CACHE_PATH: &str = const_str::concat!(DATA_FOLDER, "/", LOGIN_CACHE_FILENAME);

pub(crate) async fn run(pin_base: u64) -> Result<(), anyhow::Error> {
    let args = &(*crate::ARGS);
    const MAX_DATAGRAM_SIZE: usize = 65_507;

    let tstamp = Instant::now();
    tokio::fs::create_dir_all(DATA_FOLDER).await?;
    let (mut pid, mut login_res) = if let Some(login_res) = tokio::fs::read(LOGIN_CACHE_PATH)
        .await
        .map(|data| serde_json::from_slice::<PoltysLoginRes>(&data).ok())
        .ok()
        .flatten()
    {
        match web_login::get_pid(&login_res.address, &login_res.token).await {
            Ok(pid) => {
                log!(1, "[Login] GetPid from cache data succesfull {}", pid);
                (pid, login_res)
            }
            Err(e) => {
                log!(0, "[Login] GetPid from cache failed: {}. Relogin", e);
                (0, login_res)
            }
        }
    } else {
        (0, PoltysLoginRes::default())
    };
    if pid == 0 {
        let conn_res = web_login::poltys_connect(&args.admin, &args.user, &args.password).await?;
        log!(1, "[Login] Connect succesfull");
        login_res = web_login::login(&args.admin, &conn_res, &args.server).await?;
        log!(
            1,
            "[Login] Login succesfull {} token={}",
            login_res.address,
            login_res.token
        );
        pid = web_login::get_pid(&login_res.address, &login_res.token).await?;
        log!(1, "[Login] GetPid succesfull {}", pid);
    }
    let token = &login_res.token;
    let server_addr = args.server_addr.as_ref().unwrap_or(&login_res.address);

    let mut devices = Vec::with_capacity(args.count as usize);
    let token_pid = format!("&token={token}&pid={pid}");
    for idx in 0..args.count {
        let pin = pin_base + idx;
        if let Ok(mut device) = DCareDevice::new(server_addr).await {
            match device.initialize(pin, server_addr, &token_pid).await {
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

    let c_interval = std::time::Duration::from_millis(20);
    let mut check_timer = tokio::time::interval_at(Instant::now() + c_interval, c_interval);
    let mut data = vec![0u8; MAX_DATAGRAM_SIZE];

    loop {
        tokio::select! {
            _ = check_timer.tick() => {
                for device in devices.iter_mut() {
                    if device.need_keep_alive() {
                        device.send_keep_alive().await;
                    }

                }
            }
            _ = async {
                for device in devices.iter_mut() {
                    match device.socket.recv(&mut data).await {
                        Ok(size) => {
                            log!(3, "[DCare_{:03}] Received {size} bytes from DCare device", device.pin);
                        }
                        Err(e) => {
                            log!(1, "Error receiving data: {}", e);
                        }
                    }
                }
            } => {}
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nCtrl-C received, shutting down\n");
                break;
            }
        }
    }

    for device in devices.iter_mut() {
        if let Err(e) = device.deinitialize(server_addr, &token_pid).await {
            log!(
                1,
                "[DCare_{:03}] Deinitialization failed: {}",
                device.pin,
                e
            );
        } else {
            log!(
                2,
                "[DCare_{:03}] Device deinitialized successfully",
                device.pin
            );
        }
    }
    tokio::fs::write(LOGIN_CACHE_PATH, serde_json::to_string(&login_res)?).await?;
    Ok(())
}
