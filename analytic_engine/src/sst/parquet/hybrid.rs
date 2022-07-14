use std::{collections::BTreeMap, convert::TryFrom, mem, sync::Arc};

use arrow_deps::arrow::{
    array::{
        Array, ArrayData, ArrayRef, DictionaryArray, Float64Array, ListArray, StringArray,
        StringBuilder, TimestampMillisecondArray, UInt64Array, UInt64Builder,
    },
    buffer::MutableBuffer,
    datatypes::{
        Float64Type, Int8Type, Schema as ArrowSchema, TimeUnit, TimestampMillisecondType,
        UInt32Type, UInt64Type, UInt8Type,
    },
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::schema::{DataType, Field, Schema};
use log::debug;
use snafu::ResultExt;

use crate::sst::builder::{EncodeRecordBatch, Result};

struct RecordWrapper {
    tag_values: Vec<String>,
    timestamps: Vec<Option<i64>>,
    fields: Vec<Option<f64>>,
}

// For now, tsid/timestamp/field name is hard-coded as `tsid`, `timestamp` and
// `value`
//
// only support one field.
fn build_new_record_format(
    schema: Schema,
    tag_names: Vec<String>,
    records_by_tsid: BTreeMap<u64, RecordWrapper>,
) -> Result<ArrowRecordBatch> {
    let tsid_col = UInt64Array::from_iter_values(records_by_tsid.keys().cloned().into_iter());
    let mut ts_col = Vec::new();
    let mut fields_col = Vec::new();
    let mut tag_cols = vec![Vec::new(); tag_names.len()];

    for record_wrapper in records_by_tsid.values() {
        ts_col.push(Some(record_wrapper.timestamps.clone()));
        fields_col.push(Some(record_wrapper.fields.clone()));
        for (idx, tagv) in record_wrapper.tag_values.iter().enumerate() {
            tag_cols[idx].push(Some(tagv.as_str()));
        }
    }

    let primary_key_fields = vec![
        Field::new("tsid", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::List(Box::new(Field::new(
                "item", // this name is hard-coded in array
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ))),
            false,
        ),
    ];
    let tag_fields = tag_names
        .iter()
        .map(|n| Field::new(n, DataType::Utf8, true))
        .collect::<Vec<_>>();

    let all_fields = vec![
        primary_key_fields,
        tag_fields,
        vec![Field::new(
            "value",
            // hard code field name
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            true,
        )],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let arrow_schema = ArrowSchema::new_with_metadata(
        all_fields,
        schema.into_arrow_schema_ref().metadata().clone(),
    );

    let ts_col = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(ts_col);
    let fields_col = ListArray::from_iter_primitive::<Float64Type, _, _>(fields_col);
    let tag_cols = tag_cols
        .into_iter()
        .map(|c| Arc::new(StringArray::from(c)) as ArrayRef)
        // .map(|c| Arc::new(DictionaryArray Array::from(c)) as ArrayRef)
        // DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
        .collect::<Vec<_>>();
    let columns = vec![
        vec![Arc::new(tsid_col) as ArrayRef],
        vec![Arc::new(ts_col) as ArrayRef],
        tag_cols,
        vec![Arc::new(fields_col) as ArrayRef],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    ArrowRecordBatch::try_new(Arc::new(arrow_schema), columns)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(EncodeRecordBatch)
}

pub fn to_hybrid_record_batch(
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    let arrow_schema = arrow_record_batch_vec[0].schema();
    let schema = Schema::try_from(arrow_schema.clone()).unwrap();

    let timestamp_idx = schema.timestamp_index();
    let tsid_idx = schema.index_of_tsid();

    if tsid_idx.is_none() {
        // if table has no tsid, then return back directly.
        return ArrowRecordBatch::concat(&arrow_schema, &arrow_record_batch_vec)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch);
    }

    let tsid_idx = tsid_idx.unwrap();
    let mut tag_idxes = Vec::new();
    let mut tag_names = Vec::new();
    let mut field_idxes = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_idxes.push(idx);
            tag_names.push(col.name.clone());
        } else {
            if idx != timestamp_idx && idx != tsid_idx {
                field_idxes.push(idx);
            }
        }
    }
    debug!(
        "ts_idx:{}, tsid_idx:{}, tag_idxes:{:?}, field_idxes:{:?}",
        timestamp_idx, tsid_idx, tag_idxes, field_idxes
    );
    let mut records_by_tsid = BTreeMap::new();

    for record_batch in arrow_record_batch_vec {
        let tsid_array = record_batch
            .column(tsid_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked in build plan");

        if tsid_array.is_empty() {
            continue;
        }

        let mut tagv_columns = Vec::with_capacity(tag_idxes.len());
        for col_idx in &tag_idxes {
            let v = record_batch
                .column(*col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            tagv_columns.push(v);
        }
        let mut previous_tsid = tsid_array.value(0);
        let mut duplicated_tsids = vec![(previous_tsid, 0)]; // (tsid, offset)
        for row_idx in 1..tsid_array.len() {
            let tsid = tsid_array.value(row_idx);
            if tsid != previous_tsid {
                previous_tsid = tsid;
                duplicated_tsids.push((tsid, row_idx));
            }
        }
        for i in 0..duplicated_tsids.len() {
            let (tsid, offset) = duplicated_tsids[i];
            let length = if i == duplicated_tsids.len() - 1 {
                tsid_array.len() - offset
            } else {
                duplicated_tsids[i + 1].1 - offset
            };
            // collect timestamps
            let timestamps_in_one_tsid = record_batch.column(timestamp_idx).slice(offset, length);
            let timestamps_in_one_tsid = timestamps_in_one_tsid
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("checked in plan build");

            // collect fields
            // TODO: only collect first fields now
            let fields_in_one_tsid = record_batch.column(field_idxes[0]).slice(offset, length);
            let fields_in_one_tsid = fields_in_one_tsid
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("checked in plan build");

            let record_wrapper = records_by_tsid
                .entry(tsid)
                .or_insert_with(|| RecordWrapper {
                    // tag_idxes
                    // .iter()
                    // .enumerate()
                    // .map(|(i, tag_idx)| {
                    //     (
                    //         schema.column(*tag_idx).name.clone(),
                    //         tagv_columns[i].value(offset).to_string(),
                    //     )
                    // })
                    tag_values: tagv_columns
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect::<Vec<_>>(),
                    timestamps: vec![],
                    fields: vec![],
                });
            record_wrapper
                .timestamps
                .extend(timestamps_in_one_tsid.into_iter());
            record_wrapper.fields.extend(fields_in_one_tsid.into_iter());
        }
    }

    build_new_record_format(schema, tag_names, records_by_tsid)
}

struct TagArrayBuilder {
    num_rows: usize,
    offsets: MutableBuffer,
    values: MutableBuffer,
    offset_so_far: i32,
}

impl TagArrayBuilder {
    fn new(num_rows: usize, total_bytes: usize) -> Self {
        Self {
            num_rows,
            offsets: MutableBuffer::with_capacity(mem::size_of::<i32>() * (num_rows + 1)),
            values: MutableBuffer::with_capacity(total_bytes),
            offset_so_far: 0,
        }
    }

    fn extend_value(&mut self, value: &str, len: usize) {
        self.values.extend_from_slice(value.repeat(len).as_bytes());
        self.offsets
            .extend((self.offset_so_far..(len * value.len()) as i32).step_by(value.len()));
        self.offset_so_far += (len * value.len()) as i32;
    }

    fn build(mut self) -> ArrayRef {
        let string_array: StringArray = {
            self.offsets.push(self.values.len() as i32);
            let array_data = ArrayData::builder(DataType::Utf8)
                .len(self.num_rows)
                .add_buffer(self.offsets.into())
                .add_buffer(self.values.into());
            let array_data = unsafe { array_data.build_unchecked() };
            array_data.into()
        };
        Arc::new(string_array)
    }
}

pub fn parse_hybrid_record_batch(
    schema: Schema,
    arrow_record_batch: ArrowRecordBatch,
) -> Result<ArrowRecordBatch> {
    // let arrow_schema = arrow_record_batch.schema();
    // let schema = Schema::try_from(arrow_schema.clone()).unwrap();
    let arrow_schema = schema.to_arrow_schema_ref();
    let timestamp_idx = schema.timestamp_index();
    let tsid_idx = schema.index_of_tsid();

    if tsid_idx.is_none() {
        return Ok(arrow_record_batch);
    }

    let tsid_idx = tsid_idx.unwrap();
    let mut tag_idxes = Vec::new();
    let mut tag_names = Vec::new();
    let mut field_idxes = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_idxes.push(idx);
            tag_names.push(col.name.clone());
        } else {
            if idx != timestamp_idx && idx != tsid_idx {
                field_idxes.push(idx);
            }
        }
    }

    debug!(
        "ts_idx:{}, tsid_idx:{}, tag_idxes:{:?}, field_idxes:{:?}",
        timestamp_idx, tsid_idx, tag_idxes, field_idxes
    );
    let field_idx = field_idxes[0]; // only support one field now.

    let tsid_col = arrow_record_batch
        .column(tsid_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let timestamp_col = arrow_record_batch
        .column(timestamp_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let field_col = arrow_record_batch
        .column(field_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let tag_cols = tag_idxes
        .into_iter()
        .map(|i| {
            arrow_record_batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        })
        .collect::<Vec<_>>();

    let timestamp_array: TimestampMillisecondArray = {
        let timestamp_data = timestamp_col.data();
        let child_data = &timestamp_data.child_data()[0];
        let num_rows = child_data.len();
        let ts_buffer = &child_data.buffers()[0];
        let array_data = ArrayData::builder(DataType::Timestamp(TimeUnit::Millisecond, None))
            .len(num_rows)
            .add_buffer(ts_buffer.clone());

        unsafe { array_data.build_unchecked() }.into()
    };
    let (field_array, total_num_rows) = {
        let child_data = &field_col.data().child_data()[0];
        let num_rows = child_data.len();
        let ts_buffer = &child_data.buffers()[0];
        let array_data = ArrayData::builder(DataType::Float64)
            .len(num_rows)
            .add_buffer(ts_buffer.clone());

        (
            Float64Array::from(unsafe { array_data.build_unchecked() }),
            num_rows,
        )
    };
    let mut new_batches = Vec::with_capacity(tsid_col.len());
    let mut tag_builders = tag_cols
        .iter()
        .map(|c| {
            let mut size = 0;
            for row_idx in 0..tsid_col.len() {
                let timestamps_in_one_tsid = timestamp_col.value(row_idx);
                let tagv = c.value(row_idx);
                size += tagv.len() * timestamps_in_one_tsid.len();
            }
            TagArrayBuilder::new(total_num_rows, size)
        })
        .collect::<Vec<_>>();
    let mut tsid_buffer = MutableBuffer::with_capacity(mem::size_of::<u64>() * total_num_rows);
    for row_idx in 0..tsid_col.len() {
        let tsid = tsid_col.value(row_idx);
        let timestamps_in_one_tsid = timestamp_col.value(row_idx);
        let current_num_rows = timestamps_in_one_tsid.len();
        tsid_buffer.extend((0..current_num_rows).map(|_| tsid));

        for (idx, c) in tag_cols.iter().enumerate() {
            let tagv = c.value(row_idx);
            tag_builders[idx].extend_value(tagv, current_num_rows);
        }
    }
    let tsid_array = {
        let data = ArrayData::builder(DataType::UInt64)
            .len(total_num_rows)
            .add_buffer(tsid_buffer.into());
        let data = unsafe { data.build_unchecked() };
        UInt64Array::from(data)
    };

    let all_cols = vec![
        vec![
            Arc::new(tsid_array) as ArrayRef,
            Arc::new(timestamp_array) as ArrayRef,
        ],
        tag_builders
            .into_iter()
            .map(|b| b.build())
            .collect::<Vec<_>>(),
        vec![Arc::new(field_array) as ArrayRef],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let batch = ArrowRecordBatch::try_new(arrow_schema.clone(), all_cols).unwrap();
    new_batches.push(batch);
    ArrowRecordBatch::concat(&arrow_schema, &new_batches)
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)
}
