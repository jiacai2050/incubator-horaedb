// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Compaction picker.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use common_types::time::Timestamp;
use common_util::{config::TimeUnit, define_result};
use log::{debug, info};
use snafu::Snafu;

use crate::{
    compaction::{
        CompactionInputFiles, CompactionStrategy, CompactionTask, CompactionTaskBuilder,
        SizeTieredCompactionOptions, TimeWindowCompactionOptions,
    },
    sst::{
        file::{FileHandle, Level},
        manager::LevelsController,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

#[derive(Clone)]
pub struct PickerContext {
    pub segment_duration: Duration,
    /// The ttl of the data in sst.
    pub ttl: Option<Duration>,
    pub strategy: CompactionStrategy,
}

impl PickerContext {
    fn size_tiered_opts(&self) -> SizeTieredCompactionOptions {
        match self.strategy {
            CompactionStrategy::SizeTiered(opts) => opts,
            _ => SizeTieredCompactionOptions::default(),
        }
    }

    fn time_window_opts(&self) -> TimeWindowCompactionOptions {
        match self.strategy {
            CompactionStrategy::TimeWindow(opts) => opts,
            _ => TimeWindowCompactionOptions::default(),
        }
    }
}

pub trait CompactionPicker {
    /// Pick candidate files for compaction.
    ///
    /// Note: files being compacted should be ignored.
    fn pick_compaction(
        &self,
        ctx: PickerContext,
        levels_controller: &mut LevelsController,
    ) -> Result<CompactionTask>;
}

pub type CompactionPickerRef = Arc<dyn CompactionPicker + Send + Sync>;

trait LevelPicker {
    /// Pick candidate files for compaction at level
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>>;
}

type LevelPickerRef = Arc<dyn LevelPicker + Send + Sync>;

pub struct CommonCompactionPicker {
    level_picker: LevelPickerRef,
}

impl CommonCompactionPicker {
    pub fn new(strategy: CompactionStrategy) -> Self {
        let level_picker: LevelPickerRef = match strategy {
            CompactionStrategy::SizeTiered(_) => Arc::new(SizeTieredPicker::default()),
            CompactionStrategy::TimeWindow(_) | CompactionStrategy::Default => {
                Arc::new(TimeWindowPicker::default())
            }
        };
        Self { level_picker }
    }

    fn pick_compact_candidates(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        expire_time: Option<Timestamp>,
    ) -> Option<CompactionInputFiles> {
        for level in levels_controller.levels() {
            if let Some(files) = self.level_picker.pick_candidates_at_level(
                ctx,
                levels_controller,
                level,
                expire_time,
            ) {
                return Some(CompactionInputFiles {
                    level,
                    files,
                    output_level: level.next(),
                });
            }
        }

        None
    }
}

impl CompactionPicker for CommonCompactionPicker {
    fn pick_compaction(
        &self,
        ctx: PickerContext,
        levels_controller: &mut LevelsController,
    ) -> Result<CompactionTask> {
        let expire_time = ctx.ttl.map(Timestamp::expire_time);
        let mut builder =
            CompactionTaskBuilder::with_expired(levels_controller.expired_ssts(expire_time));

        if let Some(input_files) =
            self.pick_compact_candidates(&ctx, levels_controller, expire_time)
        {
            info!(
                "Compaction strategy: {:?} picker pick files to compact, input_files:{:?}",
                ctx.strategy, input_files
            );

            builder.add_inputs(input_files);
        }

        Ok(builder.build())
    }
}

#[inline]
fn find_uncompact_files(
    levels_controller: &LevelsController,
    level: Level,
    expire_time: Option<Timestamp>,
) -> Vec<FileHandle> {
    levels_controller
        .iter_ssts_at_level(level)
        // Only use files not being compacted and not expired.
        .filter(|file| !file.being_compacted() && !file.time_range().is_expired(expire_time))
        .cloned()
        .collect()
}

// Trim the largest sstables off the end to meet the `max_threshold` and
// `max_input_sstable_size`
fn trim_to_threshold(
    input_files: Vec<FileHandle>,
    max_threshold: usize,
    max_input_sstable_size: u64,
) -> Vec<FileHandle> {
    let mut input_size = 0;
    input_files
        .into_iter()
        .take(max_threshold)
        .take_while(|f| {
            input_size += f.size();
            input_size <= max_input_sstable_size
        })
        .collect()
}

/// Size tiered compaction strategy
/// See https://github.com/jeffjirsa/twcs/blob/master/src/main/java/com/jeffjirsa/cassandra/db/compaction/SizeTieredCompactionStrategy.java
#[derive(Default)]
pub struct SizeTieredPicker {}

/// Similar size files group
#[derive(Debug, Clone)]
struct Bucket {
    pub avg_size: usize,
    pub files: Vec<FileHandle>,
}

impl Bucket {
    fn with_file(file: &FileHandle) -> Self {
        Self {
            avg_size: file.size() as usize,
            files: vec![file.clone()],
        }
    }

    fn with_files(files: Vec<FileHandle>) -> Self {
        let total: usize = files.iter().map(|f| f.size() as usize).sum();
        let avg_size = if files.is_empty() {
            0
        } else {
            total / files.len()
        };
        Self { avg_size, files }
    }

    fn insert_file(&mut self, file: &FileHandle) {
        let total_size = self.files.len() * self.avg_size + file.size() as usize;
        self.avg_size = total_size / (self.files.len() + 1);
        self.files.push(file.clone());
    }

    fn get_hotness_map(&self) -> HashMap<FileHandle, f64> {
        self.files
            .iter()
            .map(|f| (f.clone(), Self::hotness(f)))
            .collect()
    }

    #[inline]
    fn hotness(f: &FileHandle) -> f64 {
        //prevent NAN hotness
        let row_num = f.row_num().max(1);
        f.read_meter().h2_rate() / (row_num as f64)
    }
}

impl LevelPicker for SizeTieredPicker {
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>> {
        let files_by_segment =
            Self::files_by_segment(levels_controller, level, ctx.segment_duration, expire_time);
        if files_by_segment.is_empty() {
            return None;
        }

        let all_segments: BTreeSet<_> = files_by_segment.keys().collect();
        let opts = ctx.size_tiered_opts();

        // Iterate the segment in reverse order, so newest segment is examined first.
        for (idx, segment_key) in all_segments.iter().rev().enumerate() {
            // segment_key should always exist.
            if let Some(segment) = files_by_segment.get(segment_key) {
                let buckets = Self::get_buckets(
                    segment.to_vec(),
                    opts.bucket_high,
                    opts.bucket_low,
                    opts.min_sstable_size.as_byte() as f32,
                );

                let files = Self::most_interesting_bucket(
                    buckets,
                    opts.min_threshold,
                    opts.max_threshold,
                    opts.max_input_sstable_size.as_byte(),
                );

                if files.is_some() {
                    info!(
                        "Compact segment, idx: {}, size:{}, segment_key:{:?}, files:{:?}",
                        idx,
                        segment.len(),
                        segment_key,
                        segment
                    );
                    return files;
                }
                debug!(
                    "No compaction necessary for segment, size:{}, segment_key:{:?}, idx:{}",
                    segment.len(),
                    segment_key,
                    idx
                );
            }
        }

        None
    }
}

impl SizeTieredPicker {
    ///  Group files of similar size into buckets.
    fn get_buckets(
        mut files: Vec<FileHandle>,
        bucket_high: f32,
        bucket_low: f32,
        min_sst_size: f32,
    ) -> Vec<Bucket> {
        // sort by file length
        files.sort_unstable_by_key(FileHandle::size);

        let mut buckets: Vec<Bucket> = Vec::new();
        'outer: for sst in &files {
            let size = sst.size() as f32;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than
            // `min_sst_size`)
            for bucket in buckets.iter_mut() {
                let old_avg_size = bucket.avg_size as f32;
                if (size > (old_avg_size * bucket_low) && size < (old_avg_size * bucket_high))
                    || (size < min_sst_size && old_avg_size < min_sst_size)
                {
                    // find a similar file, insert it into bucket
                    bucket.insert_file(sst);
                    continue 'outer;
                }
            }

            // no similar bucket found
            // put it in a new bucket
            buckets.push(Bucket::with_file(sst));
        }

        debug!("Group files of similar size into buckets: {:?}", buckets);

        buckets
    }

    fn most_interesting_bucket(
        buckets: Vec<Bucket>,
        min_threshold: usize,
        max_threshold: usize,
        max_input_sstable_size: u64,
    ) -> Option<Vec<FileHandle>> {
        debug!(
            "Find most_interesting_bucket buckets:{:?}, min:{}, max:{}",
            buckets, min_threshold, max_threshold
        );

        let mut pruned_bucket_and_hotness = Vec::with_capacity(buckets.len());
        // skip buckets containing less than min_threshold sstables,
        // and limit other buckets to max_threshold sstables
        for bucket in buckets {
            let (bucket, hotness) =
                Self::trim_to_threshold_with_hotness(bucket, max_threshold, max_input_sstable_size);
            if bucket.files.len() >= min_threshold {
                pruned_bucket_and_hotness.push((bucket, hotness));
            }
        }

        if pruned_bucket_and_hotness.is_empty() {
            return None;
        }

        // Find the hotest bucket
        if let Some((bucket, hotness)) =
            pruned_bucket_and_hotness
                .into_iter()
                .max_by(|(b1, h1), (b2, h2)| {
                    let c = h1.partial_cmp(h2).unwrap();
                    if !c.is_eq() {
                        return c;
                    }
                    //TODO(boyan), compacting smallest sstables first?
                    b1.avg_size.cmp(&b2.avg_size)
                })
        {
            debug!(
                "Find the hotest bucket, hotness: {}, bucket: {:?}",
                hotness, bucket
            );
            Some(bucket.files)
        } else {
            None
        }
    }

    fn files_by_segment(
        levels_controller: &LevelsController,
        level: Level,
        segment_duration: Duration,
        expire_time: Option<Timestamp>,
    ) -> HashMap<Timestamp, Vec<FileHandle>> {
        let mut files_by_segment = HashMap::new();
        let uncompact_files = find_uncompact_files(levels_controller, level, expire_time);
        for file in uncompact_files {
            // We use the end time of the range to calculate segment.
            let segment = file
                .time_range()
                .exclusive_end()
                .truncate_by(segment_duration);
            let files = files_by_segment.entry(segment).or_insert_with(Vec::new);
            files.push(file);
        }

        files_by_segment
    }

    fn trim_to_threshold_with_hotness(
        bucket: Bucket,
        max_threshold: usize,
        max_input_sstable_size: u64,
    ) -> (Bucket, f64) {
        let hotness_snapshot = bucket.get_hotness_map();

        // Sort by sstable hotness (descending).
        let mut sorted_files = bucket.files.to_vec();
        sorted_files.sort_unstable_by(|f1, f2| {
            hotness_snapshot[f1]
                .partial_cmp(&hotness_snapshot[f2])
                .unwrap()
                .reverse()
        });

        let pruned_bucket = trim_to_threshold(sorted_files, max_threshold, max_input_sstable_size);
        // bucket hotness is the sum of the hotness of all sstable members
        let bucket_hotness = pruned_bucket.iter().map(Bucket::hotness).sum();

        (Bucket::with_files(pruned_bucket), bucket_hotness)
    }
}

/// Time window compaction strategy
/// See https://github.com/jeffjirsa/twcs/blob/master/src/main/java/com/jeffjirsa/cassandra/db/compaction/TimeWindowCompactionStrategy.java
#[derive(Default)]
pub struct TimeWindowPicker {}

impl TimeWindowPicker {
    fn get_window_bounds_in_millis(window: &Duration, ts: i64) -> (i64, i64) {
        let ts_secs = ts / 1000;

        let size = window.as_secs() as i64;

        let lower = ts_secs - (ts_secs % size);
        let upper = lower + size - 1;

        (lower * 1000, upper * 1000)
    }

    #[inline]
    fn resolve_timestamp(ts: i64, timestamp_resolution: TimeUnit) -> i64 {
        match timestamp_resolution {
            TimeUnit::Microseconds => ts / 1000,
            TimeUnit::Nanoseconds => ts / 1000000,
            TimeUnit::Seconds => ts * 1000,
            TimeUnit::Milliseconds => ts,
            // the option is validated before, so it won't reach here
            _ => unreachable!(),
        }
    }

    ///  Group files of similar timestamp into buckets.
    fn get_buckets(
        files: &[FileHandle],
        window: &Duration,
        timestamp_resolution: TimeUnit,
    ) -> (HashMap<i64, Vec<FileHandle>>, i64) {
        let mut max_ts = 0i64;
        let mut buckets: HashMap<i64, Vec<FileHandle>> = HashMap::new();
        for f in files {
            let ts = f.time_range_ref().exclusive_end().as_i64();

            let ts = Self::resolve_timestamp(ts, timestamp_resolution);

            let (left, _) = Self::get_window_bounds_in_millis(window, ts);

            let bucket_files = buckets.entry(left).or_insert_with(Vec::new);

            bucket_files.push(f.clone());

            if left > max_ts {
                max_ts = left;
            }
        }

        debug!(
            "Group files of similar timestamp into buckets: {:?}",
            buckets
        );
        (buckets, max_ts)
    }

    fn newest_bucket(
        buckets: HashMap<i64, Vec<FileHandle>>,
        size_tiered_opts: SizeTieredCompactionOptions,
        now: i64,
    ) -> Option<Vec<FileHandle>> {
        // If the current bucket has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to max_threshold SSTables.

        let all_keys: BTreeSet<_> = buckets.keys().collect();

        // First compact latest buckets
        for key in all_keys.into_iter().rev() {
            if let Some(bucket) = buckets.get(key) {
                debug!("Key {}, now {}", key, now);

                let max_input_sstable_size = size_tiered_opts.max_input_sstable_size.as_byte();
                if bucket.len() >= size_tiered_opts.min_threshold && *key >= now {
                    // If we're in the newest bucket, we'll use STCS to prioritize sstables
                    let buckets = SizeTieredPicker::get_buckets(
                        bucket.to_vec(),
                        size_tiered_opts.bucket_high,
                        size_tiered_opts.bucket_low,
                        size_tiered_opts.min_sstable_size.as_byte() as f32,
                    );
                    let files = SizeTieredPicker::most_interesting_bucket(
                        buckets,
                        size_tiered_opts.min_threshold,
                        size_tiered_opts.max_threshold,
                        max_input_sstable_size,
                    );

                    if files.is_some() {
                        return files;
                    }
                } else if bucket.len() >= 2 && *key < now {
                    debug!("Bucket size {} >= 2 and not in current bucket, compacting what's here: {:?}", bucket.len(), bucket);
                    // Sort by sstable file size
                    let mut sorted_files = bucket.to_vec();
                    sorted_files.sort_unstable_by_key(FileHandle::size);
                    let candidate_files = trim_to_threshold(
                        sorted_files,
                        size_tiered_opts.max_threshold,
                        max_input_sstable_size,
                    );
                    // At least 2 sst for compaction
                    if candidate_files.len() > 1 {
                        return Some(candidate_files);
                    }
                } else {
                    debug!(
                        "No compaction necessary for bucket size {} , key {}, now {}",
                        bucket.len(),
                        key,
                        now
                    );
                }
            }
        }

        None
    }

    /// Get current window timestamp, the caller MUST ensure the level has ssts,
    /// panic otherwise.
    fn get_current_window(
        levels_controller: &LevelsController,
        level: Level,
        window: &Duration,
        timestamp_resolution: TimeUnit,
    ) -> i64 {
        // always find the latest sst here
        let now = levels_controller
            .latest_sst(level)
            .unwrap()
            .time_range()
            .exclusive_end()
            .as_i64();
        let now = Self::resolve_timestamp(now, timestamp_resolution);
        Self::get_window_bounds_in_millis(window, now).0
    }
}

impl LevelPicker for TimeWindowPicker {
    fn pick_candidates_at_level(
        &self,
        ctx: &PickerContext,
        levels_controller: &LevelsController,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Option<Vec<FileHandle>> {
        let uncompact_files = find_uncompact_files(levels_controller, level, expire_time);

        if uncompact_files.is_empty() {
            return None;
        }

        let opts = ctx.time_window_opts();

        debug!("TWCS compaction options: {:?}", opts);

        let (buckets, max_bucket_ts) = Self::get_buckets(
            &uncompact_files,
            &ctx.segment_duration,
            opts.timestamp_resolution,
        );

        let now = Self::get_current_window(
            levels_controller,
            level,
            &ctx.segment_duration,
            opts.timestamp_resolution,
        );
        debug!(
            "TWCS current window is {}, max_bucket_ts: {}",
            now, max_bucket_ts
        );
        assert!(now >= max_bucket_ts);

        Self::newest_bucket(buckets, opts.size_tiered, now)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_types::{
        bytes::Bytes,
        tests::build_schema,
        time::{TimeRange, Timestamp},
    };
    use common_util::hash_map;
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        compaction::PickerManager,
        sst::{
            file::{FileMeta, FilePurgeQueue},
            manager::{tests::LevelsControllerMockBuilder, LevelsController},
            meta_data::SstMetaData,
            parquet::meta_data::ParquetMetaData,
        },
        table_options::StorageFormat,
    };

    fn build_sst_meta_data(time_range: TimeRange) -> SstMetaData {
        let parquet_meta_data = ParquetMetaData {
            min_key: Bytes::from_static(b"100"),
            max_key: Bytes::from_static(b"200"),
            time_range,
            max_sequence: 200,
            schema: build_schema(),
            parquet_filter: Default::default(),
            collapsible_cols_idx: Vec::new(),
        };

        SstMetaData::Parquet(Arc::new(parquet_meta_data))
    }

    // testcase 0: file buckets: old bucket:[0,1] newest bucket:[2], expired:[3]
    fn build_old_bucket_case(now: i64) -> LevelsController {
        let builder = LevelsControllerMockBuilder::default();
        let sst_meta_vec = vec![
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 14000),
                Timestamp::new(now - 13000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 14000),
                Timestamp::new(now - 13000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(100),
                Timestamp::new(200),
            )),
        ];
        builder.add_sst(sst_meta_vec).build()
    }

    // testcase 1: file buckets: old bucket:[0,1] newest bucket:[2,3,4,5]
    // default min_threshold=4
    fn build_newest_bucket_case(now: i64) -> LevelsController {
        let builder = LevelsControllerMockBuilder::default();
        let sst_meta_vec = vec![
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 14000),
                Timestamp::new(now - 13000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 14000),
                Timestamp::new(now - 13000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
        ];
        builder.add_sst(sst_meta_vec).build()
    }

    // testcase 2: file buckets: old bucket:[0] newest bucket:[1,2,3]
    // default min_threshold=4
    fn build_newest_bucket_no_match_case(now: i64) -> LevelsController {
        let builder = LevelsControllerMockBuilder::default();
        let sst_meta_vec = vec![
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 14000),
                Timestamp::new(now - 13000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
            build_sst_meta_data(TimeRange::new_unchecked(
                Timestamp::new(now - 4000),
                Timestamp::new(now - 3000),
            )),
        ];
        builder.add_sst(sst_meta_vec).build()
    }

    #[test]
    fn test_time_window_picker() {
        let picker_manager = PickerManager::default();
        let twp = picker_manager.get_picker(CompactionStrategy::Default);
        let mut ctx = PickerContext {
            segment_duration: Duration::from_millis(1000),
            ttl: Some(Duration::from_secs(100000)),
            strategy: CompactionStrategy::Default,
        };
        let now = Timestamp::now();
        {
            let mut lc = build_old_bucket_case(now.as_i64());
            let task = twp.pick_compaction(ctx.clone(), &mut lc).unwrap();
            assert_eq!(task.inputs[0].files.len(), 2);
            assert_eq!(task.inputs[0].files[0].id(), 0);
            assert_eq!(task.inputs[0].files[1].id(), 1);
            assert_eq!(task.expired[0].files.len(), 1);
            assert_eq!(task.expired[0].files[0].id(), 3);
        }

        {
            let mut lc = build_newest_bucket_case(now.as_i64());
            let task = twp.pick_compaction(ctx.clone(), &mut lc).unwrap();
            assert_eq!(task.inputs[0].files.len(), 4);
            assert_eq!(task.inputs[0].files[0].id(), 2);
            assert_eq!(task.inputs[0].files[1].id(), 3);
            assert_eq!(task.inputs[0].files[2].id(), 4);
            assert_eq!(task.inputs[0].files[3].id(), 5);
        }

        {
            let mut lc = build_newest_bucket_no_match_case(now.as_i64());
            let task = twp.pick_compaction(ctx.clone(), &mut lc).unwrap();
            assert_eq!(task.inputs.len(), 0);
        }

        // If ttl is None, then no file is expired.
        ctx.ttl = None;
        {
            let mut lc = build_old_bucket_case(now.as_i64());
            let task = twp.pick_compaction(ctx, &mut lc).unwrap();
            assert_eq!(task.inputs[0].files.len(), 2);
            assert_eq!(task.inputs[0].files[0].id(), 0);
            assert_eq!(task.inputs[0].files[1].id(), 1);
            assert!(task.expired[0].files.is_empty());
        }
    }

    fn build_file_handles(sizes: Vec<(u64, TimeRange)>) -> Vec<FileHandle> {
        let (tx, _rx) = mpsc::unbounded_channel();

        sizes
            .into_iter()
            .map(|(size, time_range)| {
                let file_meta = FileMeta {
                    size,
                    time_range,
                    id: 1,
                    row_num: 0,
                    max_seq: 0,
                    storage_format: StorageFormat::default(),
                };
                let queue = FilePurgeQueue::new(1, 1.into(), tx.clone());
                FileHandle::new(file_meta, queue)
            })
            .collect()
    }

    #[test]
    fn test_size_tiered_picker() {
        let time_range = TimeRange::empty();
        let bucket = Bucket::with_files(build_file_handles(vec![
            (100, time_range),
            (110, time_range),
            (200, time_range),
        ]));

        let (out_bucket, _) =
            SizeTieredPicker::trim_to_threshold_with_hotness(bucket.clone(), 10, 300);
        // limited by max input size
        assert_eq!(
            vec![100, 110],
            out_bucket
                .files
                .iter()
                .map(|f| f.size())
                .collect::<Vec<_>>()
        );

        // no limit
        let (out_bucket, _) =
            SizeTieredPicker::trim_to_threshold_with_hotness(bucket.clone(), 10, 3000);
        assert_eq!(
            vec![100, 110, 200],
            out_bucket
                .files
                .iter()
                .map(|f| f.size())
                .collect::<Vec<_>>()
        );

        // limited by max_threshold
        let (out_bucket, _) = SizeTieredPicker::trim_to_threshold_with_hotness(bucket, 2, 3000);
        assert_eq!(
            vec![100, 110],
            out_bucket
                .files
                .iter()
                .map(|f| f.size())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_bucket() {
        let bucket = Bucket::with_files(vec![]);
        assert_eq!(bucket.avg_size, 0);
        assert!(bucket.files.is_empty());
    }

    #[test]
    fn test_time_window_newest_bucket() {
        let size_tiered_opts = SizeTieredCompactionOptions::default();
        // old bucket have enough sst for compaction
        {
            let old_bucket = build_file_handles(vec![
                (102, TimeRange::new_unchecked_for_test(100, 200)),
                (100, TimeRange::new_unchecked_for_test(100, 200)),
                (101, TimeRange::new_unchecked_for_test(100, 200)),
            ]);
            let new_bucket = build_file_handles(vec![
                (200, TimeRange::new_unchecked_for_test(200, 300)),
                (201, TimeRange::new_unchecked_for_test(200, 300)),
            ]);

            let buckets = hash_map! { 100 => old_bucket, 200 => new_bucket };
            let bucket = TimeWindowPicker::newest_bucket(buckets, size_tiered_opts, 200).unwrap();
            assert_eq!(
                vec![100, 101, 102],
                bucket.into_iter().map(|f| f.size()).collect::<Vec<_>>()
            );
        }

        // old bucket have only 1 sst, which is not enough for compaction
        {
            let old_bucket =
                build_file_handles(vec![(100, TimeRange::new_unchecked_for_test(100, 200))]);
            let new_bucket = build_file_handles(vec![
                (200, TimeRange::new_unchecked_for_test(200, 300)),
                (201, TimeRange::new_unchecked_for_test(200, 300)),
            ]);

            let buckets = hash_map! { 100 => old_bucket, 200 => new_bucket };
            let bucket = TimeWindowPicker::newest_bucket(buckets, size_tiered_opts, 200);
            assert_eq!(None, bucket);
        }
    }
}
