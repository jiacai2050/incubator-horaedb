use std::{fs::File, sync::Arc, time::Instant};

use analytic_engine::sst::{
    factory::{SstReaderOptions, SstType},
    parquet::reader::ParquetSstReader,
    reader::SstReader,
};
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::DatumKind,
    projected_schema::ProjectedSchema,
    schema::{Builder as SchemaBuilder, Schema},
};
use futures::StreamExt;
use object_store::{LocalFileSystem, Path};
use parquet::{DataCacheRef, MetaCacheRef};
use table_engine::predicate::Predicate;

use crate::util;

fn build_column_schema(column_name: &str, data_type: DatumKind, is_tag: bool) -> ColumnSchema {
    let builder = column_schema::Builder::new(column_name.to_string(), data_type)
        .is_nullable(true)
        .is_tag(is_tag);

    builder.build().unwrap()
}

fn build_schema() -> Schema {
    let mut builder = SchemaBuilder::new().auto_increment_column_id(true);
    builder = builder
        .enable_tsid_primary_key(true)
        .add_key_column(
            column_schema::Builder::new("tsid".to_string(), DatumKind::UInt64)
                .is_nullable(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                .is_nullable(false)
                .build()
                .unwrap(),
        )
        .unwrap();
    let tag_cols = vec![
        "arch",
        "datacenter",
        "hostname",
        "os",
        "rack",
        "region",
        "service",
        "service_environment",
        "service_version",
        "team",
    ];
    for col in tag_cols {
        builder = builder
            .add_normal_column(build_column_schema(col, DatumKind::String, true))
            .unwrap();
    }

    builder = builder
        .add_normal_column(build_column_schema("value", DatumKind::Double, false))
        .unwrap();
    builder.build().unwrap()
}

pub fn run(path: &str, sst_name: &str) {
    let store = LocalFileSystem::new_with_prefix(path).unwrap();
    let runtime = Arc::new(util::new_runtime(2));

    let sst_path = Path::from(sst_name);
    let meta_cache: Option<MetaCacheRef> = None;
    let data_cache: Option<DataCacheRef> = None;

    let schema = build_schema();
    let projected_schema = ProjectedSchema::new(schema.clone(), Some((0..13).collect())).unwrap();
    let sst_reader_options = SstReaderOptions {
        sst_type: SstType::Parquet,
        read_batch_row_num: 8192,
        reverse: false,
        projected_schema,
        predicate: Arc::new(Predicate::empty()),
        meta_cache: meta_cache.clone(),
        data_cache: data_cache.clone(),
        runtime: runtime.clone(),
    };
    runtime.block_on(async {
        let now = Instant::now();
        let mut reader = ParquetSstReader::new(&sst_path, &store, &sst_reader_options);
        let mut streams = reader.read().await.unwrap();
        let mut total_rows = 0;
        while let Some(item) = streams.next().await {
            match item {
                Err(e) => println!("err: {}", e),
                Ok(b) => total_rows += b.num_rows(), // println!("batch: {:?}", b),
            }
        }
        let cost = Instant::now().duration_since(now);
        println!("done. {}ms {}", cost.as_millis(), total_rows);
    });
}
