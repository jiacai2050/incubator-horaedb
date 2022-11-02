// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Once;

use benchmarks::{
    config::{self, BenchConfig},
    parquet_bench::ParquetBench,
};

static INIT_LOG: Once = Once::new();

pub fn init_bench() -> BenchConfig {
    INIT_LOG.call_once(|| {
        env_logger::init();
    });

    config::bench_config_from_env()
}

fn main() {
    let config = init_bench();
    let bench = ParquetBench::new(config.sst_bench);

    for i in 0..10 {
        println!("{i}...");
        bench.run_bench();
    }
    println!("done");
}
