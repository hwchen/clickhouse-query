use clickhouse_rs::{Pool, types::Block};
use structopt::StructOpt;
use tokio::prelude::*;

fn main() {
    let opt = CliOpt::from_args();

    let address = opt.address;
    let query = opt.query;

    let pool = Pool::new(address.as_str());
    let done = pool
        .get_handle()
        .and_then(|c| c.ping())
        .and_then(move |c| c.query(&query).fetch_all())
        .and_then(move |(_, block)| {
            println!("{:?}", block);
            Ok(())
        })
        .map_err(|err| println!("database error: {}", err));

    tokio::run(done)
}

#[derive(Debug, StructOpt)]
struct CliOpt {
    #[structopt(short="a")]
    address: String,
    query: String,
}
