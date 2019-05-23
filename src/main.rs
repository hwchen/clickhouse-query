use clickhouse_rs::{Pool, types::{Block, SqlType}};
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
            for row in block.rows() {
                for i in 0..row.len() {
                    match row.sql_type(i) {
                        Ok(SqlType::UInt8) => {
                            let val: u8 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::UInt16) => {
                            let val: u16 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::UInt32) => {
                            let val: u32 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::UInt64) => {
                            let val: u64 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Int8) => {
                            let val: i8 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Int16) => {
                            let val: i16 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Int32) => {
                            let val: i32 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Int64) => {
                            let val: i64 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::String) => {
                            let val: &str = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::FixedString(usize)) => {
                            let val: &str = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Float32) => {
                            let val: f32 = row.get(i)?;
                            println!("{}", val);
                        },
                        Ok(SqlType::Float64) => {
                            let val: f64 = row.get(i)?;
                            println!("{}", val);
                        },
                        _ => panic!(),
                    };
                }
            }
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
