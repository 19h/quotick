// quotick/src/main.rs
use anyhow::Result;
use quotick::prelude::*;
use std::fs;
use std::path::Path;

const TEST_DATA_DIR: &str = "./test_data/qtb-db";
const OUTPUT_FILE: &str = "./test_data/qtb-db/example.qtb";

/// Generates a sample OHLCV record for a given instrument ID and timestamp.
fn make_ohlcv_record(instrument_id: u32, ts_event: u64) -> OhlcvMsg {
    OhlcvMsg::new(
        1, // publisher_id
        instrument_id,
        ts_event,
        15000, // open: Prices are fixed-point integers
        15100, // high
        14900, // low
        15050, // close
        1_000_000, // volume
    )
}

/// Creates and writes a sample `.qtb` file.
fn write_example_file() -> Result<()> {
    println!("--- Writing Example QTB File ---");

    // Ensure the output directory exists
    let path = Path::new(OUTPUT_FILE);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let symbols = &["AAPL", "MSFT"];
    let mut store = QtbStore::file_create(path, Schema::Ohlcv, symbols)?;

    println!("Created store for symbols: {:?}", symbols);

    let mut records = Vec::new();
    let base_ts = 1672531200_000_000_000; // 2023-01-01 00:00:00 UTC

    // Generate 5 records for AAPL (instrument_id 1)
    for i in 0..5 {
        records.push(make_ohlcv_record(1, base_ts + i * 1_000_000_000));
    }
    // Generate 5 records for MSFT (instrument_id 2)
    for i in 0..5 {
        records.push(make_ohlcv_record(2, base_ts + i * 1_000_000_000));
    }

    for record in &records {
        store.write_record(record)?;
    }
    println!("Wrote {} records.", records.len());

    store.finish()?;
    println!("Finalized and closed file: {}", OUTPUT_FILE);
    println!();
    Ok(())
}

/// Reads and verifies the sample `.qtb` file.
fn read_example_file() -> Result<()> {
    println!("--- Reading Example QTB File ---");

    let store = QtbStore::file_open(OUTPUT_FILE)?;
    let metadata = store.metadata();

    println!("Successfully opened file and parsed metadata.");
    println!("  Version: {}", metadata.version());
    println!("  Schema: {}", metadata.schema());
    println!("  Symbols: {:?}", metadata.symbols);
    println!(
        "  Timestamp range: {} to {}",
        metadata.start(),
        metadata.end()
    );
    println!("  Record count: {}", store.record_count());
    println!();

    let mut record_count = 0;
    println!("Iterating through OHLCV records...");
    // Use the safe, borrowing iterator `iter`
    for record_result in store.iter::<OhlcvMsg>() {
        let record = record_result?;
        println!(
            "  Read record for instrument_id {}: {:?}",
            record.header().instrument_id(),
            record
        );
        record_count += 1;
    }

    println!(
        "\nFinished reading. Total records processed: {}",
        record_count
    );
    assert_eq!(
        record_count, 10,
        "Mismatch in record count"
    );
    println!("Record count verified successfully.");
    Ok(())
}

fn main() -> Result<()> {
    // Clean up previous runs if necessary
    if Path::new(TEST_DATA_DIR).exists() {
        fs::remove_dir_all(TEST_DATA_DIR)?;
    }

    write_example_file()?;
    read_example_file()?;

    println!("\n✅ QTB round-trip example completed successfully!");
    Ok(())
}
