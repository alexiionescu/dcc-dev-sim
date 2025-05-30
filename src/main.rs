use anyhow::Ok;
use clap::{ArgAction, Parser, Subcommand, command};

#[derive(Parser)]
pub(crate) struct Args {
    #[command(subcommand)]
    cmd: ArgsCommand,
    #[clap(short = 'v', action = ArgAction::Count, help = "verbosity level (e.g. -vvv)")]
    verbosity: u8,
    #[clap(short = 'A', long, help = "admin hosting SSL host:port")]
    admin: String,
    #[clap(short = 'S', long, help = "server instance name")]
    server: String,
    #[clap(long, help = "server SSL address override")]
    server_addr: Option<String>,
    #[clap(short = 'C', long, help = "count to simulate", default_value = "1")]
    count: usize,
    #[clap(short = 'U', long, help = "user name for authentication")]
    user: String,
    #[clap(short = 'P', long, help = "password for authentication")]
    password: String,
    #[clap(
        short = 'B',
        long,
        help = "base device id (pin for DCare, mac address for CH, etc.)"
    )]
    dev_id_base: usize,
    #[clap(short = 'j', help = "number of parallel jobs", default_value = "1")]
    concurrent_jobs: usize,
}

mod devices;
use devices::*;
use once_cell::sync::Lazy;

#[derive(Subcommand)]
enum ArgsCommand {
    #[clap(name = "dcare", about = "Run DCare device simulation")]
    DCare {},
    #[clap(about = "Run Comm Hub device simulation")]
    CommHub {
        #[clap(short = 'p', long, help = "port number", default_value = "19398")]
        port: u16,
    },
}

mod utils;

static ARGS: Lazy<Args> = Lazy::new(Args::parse);
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    match ARGS.cmd {
        ArgsCommand::DCare {} => {
            dcare::run().await?;
        }
        ArgsCommand::CommHub { port } => {
            comm_hub::run(port).await?;
        }
    }
    Ok(())
}
