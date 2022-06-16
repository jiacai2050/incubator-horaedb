use std::{collections::BTreeMap, convert::TryFrom, sync::Arc};

use arrow_deps::arrow::{
    array::{Float64Array, ListArray, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{Float64Type, Schema as ArrowSchema, TimestampMillisecondType},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::schema::{DataType, Field, Schema};
use snafu::ResultExt;

use crate::sst::builder::{EncodeRecordBatch, Result};

struct RecordWrapper {
    tag_values: Vec<String>,
    timestamps: Vec<Option<i64>>,
    fields: Vec<Option<f64>>,
}

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
            tag_cols[idx].push(Some(tagv));
        }
    }

    let mut all_fields = vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("tsid", DataType::UInt64, false),
    ];
    let tag_fields = tag_names
        .iter()
        .map(|n| Field::new(n, DataType::Utf8, true))
        .collect::<Vec<_>>();
    all_fields.extend(tag_fields);
    // hard code field name
    all_fields.push(Field::new("value", DataType::Float64, false));

    let arrow_schema = ArrowSchema::new_with_metadata(
        all_fields,
        schema.into_arrow_schema_ref().metadata().clone(),
    );

    let ts_col = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(ts_col);
    let fields_col = ListArray::from_iter_primitive::<Float64Type, _, _>(fields_col);
    ArrowRecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(ts_col), Arc::new(tsid_col), Arc::new(fields_col)],
    )
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    .context(EncodeRecordBatch)
}

pub fn to_hybrid_record_batch(
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    let arrow_schema = arrow_record_batch_vec[0].schema();
    let schema = Schema::try_from(arrow_schema.clone()).unwrap();

    let tsid_idx = schema.index_of_tsid().unwrap();
    let timestamp_idx = schema.timestamp_index();

    let mut tag_idxes = Vec::new();
    let mut tag_names = Vec::new();
    let mut field_idxes = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_idxes.push(idx);
            tag_names.push(col.name.clone());
        } else {
            field_idxes.push(idx);
        }
    }

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
                    //                     tag_idxes
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
    // let ret = ArrowRecordBatch::concat(&arrow_schema,
    // &arrow_record_batch_vec)     .map_err(|e| Box::new(e) as _)
    //     .context(EncodeRecordBatch)?;
    // Ok(ret)
}
