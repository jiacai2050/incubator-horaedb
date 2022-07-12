use benchmarks::hybrid_bench;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};

fn bench_hybrid_sst_enable(c: &mut Criterion) {
    let path = std::env::var("TEST_PATH")
        .unwrap_or("/Users/jiacai/tt/yes-hybrid2/store/2/2199023255553/".to_string());
    let sst_name = std::env::var("TEST_SST").unwrap_or("1.sst".to_string());
    c.bench_function("hybrid_sst_enable", |b| {
        b.iter(|| hybrid_bench::run(black_box(&path), &sst_name))
    });
}

fn bench_hybrid_sst_disable(c: &mut Criterion) {
    let path = std::env::var("TEST_PATH")
        .unwrap_or("/Users/jiacai/tt/no-hybrid/store/2/2199023255553/".to_string());
    let sst_name = std::env::var("TEST_SST").unwrap_or("1.sst".to_string());
    c.bench_function("hybrid_sst_disable", |b| {
        b.iter(|| hybrid_bench::run(black_box(&path), &sst_name))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_hybrid_sst_enable , bench_hybrid_sst_disable,
}

criterion_main!(benches);

#[cfg(flame_test)]
fn main() {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(100)
        // .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();
    println!("hello");
    let args = std::env::args().collect::<Vec<String>>();
    let path = &args[1];
    let sst_name = &args[2];
    for _ in 0..10 {
        hybrid_bench::run(path, sst_name);
    }
    if let Ok(report) = guard.report().build() {
        let file = std::fs::File::create("/tmp/flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
}
// fn main() {
//     println!("hello");
//     let args = std::env::args().collect::<Vec<String>>();
//     let path = &args[1];
//     let sst_name = &args[2];
//     run(path, sst_name)
// }

// fn run(path: &str, sst_name: &str) {
//     println!("{path} {sst_name}");
//     let store = LocalFileSystem::new_with_prefix(path).unwrap();
//     let runtime = Arc::new(util::new_runtime(2));

//     let sst_path = Path::from(sst_name);
//     let meta_cache: Option<MetaCacheRef> = None;
//     let data_cache: Option<DataCacheRef> = None;

//     // let schema = runtime.block_on(util::schema_from_sst(
//     //     &store,
//     //     &sst_path,
//     //     &meta_cache,
//     //     &data_cache,
//     // ));
//     let schema = build_schema();
//     let projected_schema = ProjectedSchema::new(schema.clone(),
// Some((0..13).collect())).unwrap();     let sst_reader_options =
// SstReaderOptions {         sst_type: SstType::Parquet,
//         read_batch_row_num: 8192,
//         reverse: false,
//         projected_schema,
//         predicate: Arc::new(Predicate::empty()),
//         meta_cache: meta_cache.clone(),
//         data_cache: data_cache.clone(),
//         runtime: runtime.clone(),
//     };
//     let guard = pprof::ProfilerGuardBuilder::default()
//         .frequency(100)
//         .blocklist(&["libc", "libgcc", "pthread", "vdso"])
//         .build()
//         .unwrap();

//     runtime.block_on(async {
//         let now = Instant::now();
//         let mut reader = ParquetSstReader::new(&sst_path, &store,
// &sst_reader_options);         let mut streams = reader.read().await.unwrap();
//         let mut total_rows = 0;
//         while let Some(item) = streams.next().await {
//             match item {
//                 Err(e) => println!("err: {}", e),
//                 Ok(b) => total_rows += b.num_rows(), // println!("batch:
// {:?}", b),             }
//         }
//         let cost = Instant::now().duration_since(now);
//         println!("done. {}ms {}", cost.as_millis(), total_rows);
//     });
//     if let Ok(report) = guard.report().build() {
//         let file = File::create("/tmp/flamegraph.svg").unwrap();
//         report.flamegraph(file).unwrap();
//     };
// }
