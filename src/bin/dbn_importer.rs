// quotick/src/bin/dbn_importer.rs
//! A command-line utility to convert Databento Binary Encoding (DBN) files
//! to the Quotick Binary (QTB) format.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use quotick::importer::Converter;

/// A high-performance tool to convert DBN files to the QTB format.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// The path to the input DBN file.
    #[clap(value_parser)]
    input: PathBuf,

    /// The path for the output QTB file.
    #[clap(value_parser)]
    output: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!(
        "Converting DBN file '{}' to QTB file '{}'...",
        cli.input.display(),
        cli.output.display()
    );

    // Execute the two-pass conversion process.
    Converter::run(&cli.input, &cli.output)?;

    println!("\n✅ Conversion completed successfully!");

    Ok(())
}
