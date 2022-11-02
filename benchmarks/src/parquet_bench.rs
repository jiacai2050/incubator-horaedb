// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Parquet bench.

use std::{sync::Arc, time::Instant};

use arrow::array::Int64Array;
use common_types::schema::Schema;
use common_util::runtime::Runtime;
use futures::TryStreamExt;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef, Path};
use parquet::arrow::{
    arrow_reader::ParquetRecordBatchReaderBuilder, ParquetRecordBatchStreamBuilder,
};
use parquet_ext::{DataCacheRef, MetaCacheRef};
use table_engine::predicate::PredicateRef;
use tokio::fs::File;

use crate::{config::SstBenchConfig, util};

pub struct ParquetBench {
    store: ObjectStoreRef,
    store_path: String,
    pub sst_file_name: String,
    max_projections: usize,
    projection: Vec<usize>,
    _schema: Schema,
    _predicate: PredicateRef,
    runtime: Arc<Runtime>,
    is_async: bool,
    batch_size: usize,
}

impl ParquetBench {
    pub fn new(config: SstBenchConfig) -> Self {
        let store = Arc::new(LocalFileSystem::new_with_prefix(&config.store_path).unwrap()) as _;

        let runtime = util::new_runtime(config.runtime_thread_num);

        let sst_path = Path::from(config.sst_file_name.clone());
        let meta_cache: Option<MetaCacheRef> = None;
        let data_cache: Option<DataCacheRef> = None;

        let schema = runtime.block_on(util::schema_from_sst(
            &store,
            &sst_path,
            &meta_cache,
            &data_cache,
        ));

        ParquetBench {
            store,
            store_path: config.store_path,
            sst_file_name: config.sst_file_name,
            max_projections: config.max_projections,
            projection: Vec::new(),
            _schema: schema,
            _predicate: config.predicate.into_predicate(),
            runtime: Arc::new(runtime),
            is_async: config.is_async,
            batch_size: config.read_batch_row_num,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projection = if i < self.max_projections {
            (0..i + 1).into_iter().collect()
        } else {
            Vec::new()
        };

        self.projection = projection;
    }

    pub fn run_bench(&self) {
        if self.is_async {
            return self.run_async_bench();
        }

        self.run_sync_bench()
    }

    pub fn run_sync_bench(&self) {
        let sst_path = Path::from(self.sst_file_name.clone());

        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let get_result = self.store.get(&sst_path).await.unwrap();

            let open_cost = open_instant.elapsed();

            let filter_begin_instant = Instant::now();
            let arrow_reader =
                ParquetRecordBatchReaderBuilder::try_new(get_result.bytes().await.unwrap())
                    .unwrap()
                    .with_batch_size(self.batch_size)
                    .build()
                    .unwrap();
            let filter_cost = filter_begin_instant.elapsed();

            let iter_begin_instant = Instant::now();
            let mut total_rows = 0;
            let mut batch_num = 0;
            let mut hit = 0;
            for record_batch in arrow_reader {
                let batch = record_batch.unwrap();
                let index_code = &batch.columns()[3];
                let index_code = index_code.as_any().downcast_ref::<Int64Array>().unwrap();
                if index_code.values().contains(&55489) {
                    hit += 1;
                }
                let num_rows = batch.num_rows();

                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench Sync total rows of sst:{}, batch:{}, hit:{}, hit/batch:{},
                open cost:{:?}, filter cost:{:?}, iter cost:{:?}",
                total_rows,
                batch_num,
                hit,
                hit as f64 / batch_num as f64,
                open_cost,
                filter_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }

    pub fn run_async_bench(&self) {
        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let file = File::open(format!("{}/{}", self.store_path, self.sst_file_name))
                .await
                .expect("failed to open file");

            let open_cost = open_instant.elapsed();

            let filter_begin_instant = Instant::now();
            let stream = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .unwrap()
                .with_batch_size(self.batch_size)
                .build()
                .unwrap();
            let filter_cost = filter_begin_instant.elapsed();

            let mut total_rows = 0;
            let mut batch_num = 0;
            let iter_begin_instant = Instant::now();
            for record_batch in stream.try_collect::<Vec<_>>().await.unwrap() {
                let num_rows = record_batch.num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench Async total rows of sst:{}, total batch num:{},
                open cost:{:?}, filter cost:{:?}, iter cost:{:?}",
                total_rows,
                batch_num,
                open_cost,
                filter_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }
}
