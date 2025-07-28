// src/arrow_structs.rs

use anyhow::{Context, Result};
use std::{collections::HashMap, fs::File, sync::Arc};

// Arrow core
use arrow::{
    array::StructArray,
    compute::cast,
    datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};

// Derive macros + traits for converting your Rust structs → Arrow
use arrow_convert::serialize::TryIntoArrow;
use arrow_convert::{ArrowField, ArrowSerialize};

// Parquet writer from the official parquet crate
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use arrow::array::ArrayRef;

// Protobuf types for block rewards
use crate::pb::sf::solana::raw::blocks::rewards::v1::{BlockReward, Output};

////////////////////////////////////////////////////////////////////////////////
// 1) Wrapper struct for Arrow serialization
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ArrowField, ArrowSerialize)]
pub struct BlockRewardWrap {
    pub block_date: String,
    pub block_slot: u64,
    #[arrow_field(data_type = "Timestamp(Millisecond, None)")]
    pub block_timestamp: i64,
    pub pubkey: String,
    pub lamports: u64,
    pub post_balance: u64,
    pub reward_type: String,
    pub commission: u64,
    pub block_hash: String,
}

////////////////////////////////////////////////////////////////////////////////
// 2) Conversion from Protobuf
////////////////////////////////////////////////////////////////////////////////

impl From<&BlockReward> for BlockRewardWrap {
    fn from(b: &BlockReward) -> Self {
        Self {
            block_date: b.block_date.clone(),
            block_slot: b.block_slot,
            block_timestamp: (b.block_timestamp as i64) * 1000,
            pubkey: b.pubkey.clone(),
            lamports: b.lamports,
            post_balance: b.post_balance,
            reward_type: b.reward_type.clone(),
            commission: b.commission,
            block_hash: b.block_hash.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// 3) Write a slice of BlockReward into a single Parquet file
////////////////////////////////////////////////////////////////////////////////

pub fn flush_block_rewards_parquet(rows: &[BlockReward], path: &str) -> Result<()> {
    // a) wrap + schema
    let wrapped: Vec<BlockRewardWrap> = rows.iter().map(From::from).collect();
    let original_schema = BlockRewardWrap::arrow_schema();
    let metadata = original_schema.metadata().clone();
    let new_fields: Vec<FieldRef> = original_schema
        .fields()
        .iter()
        .map(|f_ref| {
            if f_ref.name() == "block_timestamp" {
                Arc::new(Field::new(
                    "block_timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    f_ref.is_nullable(),
                ))
            } else {
                f_ref.clone()
            }
        })
        .collect();

    let schema = ArrowSchema::new_with_metadata(new_fields, metadata);
    let array: ArrayRef = wrapped.try_into_arrow()?;
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("StructArray");

    // b) RecordBatch
    let mut columns = struct_array.columns().to_vec();
    if let Ok(idx) = schema.index_of("block_timestamp") {
        columns[idx] = cast(&columns[idx], &DataType::Timestamp(TimeUnit::Millisecond, None))?;
    }
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

    // c) write
    let file = File::create(path)?;
    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// 4) Group by `block_date` and write partitioned Parquet files
////////////////////////////////////////////////////////////////////////////////

pub fn flush_partitioned(rows: Vec<BlockReward>) -> Result<()> {
    // 1) group by the String key `block_date`
    let mut groups: HashMap<String, Vec<BlockReward>> = HashMap::new();
    for br in rows {
        groups.entry(br.block_date.clone()).or_default().push(br);
    }

    // 2) for each partition, mkdir + write
    for (date, group) in groups {
        let dir = format!("./out/block_date={}/", date);
        std::fs::create_dir_all(&dir).context(format!("creating partition dir `{}`", dir))?;

        let min_slot = group.iter().map(|r| r.block_slot).min().unwrap();
        let max_slot = group.iter().map(|r| r.block_slot).max().unwrap();

        let path = format!("{}/{}_{}.parquet", dir.trim_end_matches('/'), min_slot, max_slot);
        flush_block_rewards_parquet(&group, &path)?;
    }

    Ok(())
}
