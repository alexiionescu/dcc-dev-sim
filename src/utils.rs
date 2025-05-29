#[macro_export]
macro_rules! log_prefix {
    ($level:expr) => {
        match $level {
            0 => "",
            1 => "  ",
            2 => "    ",
            3 => "        ",
            _ => "            ",
        }
    };
}

#[macro_export]
macro_rules! log(
    ($level:expr, $($arg:tt)*) => {
        if ($level as u8) <= (*$crate::ARGS).verbosity {
            println!("{}\t{}{}",chrono::Local::now().format("%Y-%m-%d %H:%M:%S.%3f"),$crate::log_prefix!($level as u8), format!($($arg)*));
        }
    };
);
