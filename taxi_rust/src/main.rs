use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use clap::Parser;
use clickhouse::{Client, Row};
use colored::*;
use polars::prelude::*;
use serde::Serialize;
use std::path::Path;
use std::time::Instant;
use url::Url;

//======================================================================
// Part 1: Reconstructed Structs and Main Application Logic
// These parts were missing from your file, causing the primary errors.
//======================================================================

/// A CLI application to upload NYC Taxi data to ClickHouse.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'd', long, help = "Path to the directory with Parquet files")]
    data_path: String,
    
    #[arg(short = 'u', long, help = "ClickHouse database connection URL")]
    db_url: String,

    #[arg(short, long, help = "Table name to upload to")]
    table_name: String,
}

/// Holds statistics about the data processing job.
#[derive(Debug)]
struct ProcessingStats {
    start_time: Instant,
    files_processed: u32,
    files_failed: u32,
    rows_processed: u64,
    rows_uploaded: u64,
    rows_filtered: u64,
    data_processed_mb: f64,
}

/// The main application context, holding shared state like the database client.
/// This is the `self` context for your methods.
struct AppContext {
    client: Client,
    stats: ProcessingStats,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Parse the URL from the command line argument
    let parsed_url = Url::parse(&args.db_url).context("Failed to parse database URL")?;

    // Extract components for the ClickHouse client
    let host = parsed_url.host_str().context("URL has no host")?;
    let port = parsed_url.port().context("URL has no port")?;
    let user = parsed_url.username();
    let password = parsed_url.password().unwrap_or("");
    let db_name = parsed_url.path_segments().and_then(|mut p| p.next()).unwrap_or("default");
    
    // Determine the scheme (http/https)
    let scheme = if parsed_url.query_pairs().any(|(k, v)| k == "secure" && v == "true") {
        "https"
    } else {
        "http"
    };

    let client_url = format!("{}://{}:{}", scheme, host, port);
    
    // Construct the ClickHouse client
    let client = Client::default()
        .with_url(&client_url)
        .with_user(user)
        .with_password(password)
        .with_database(db_name);

    let app = AppContext {
        client,
        stats: ProcessingStats {
            start_time: Instant::now(),
            files_processed: 0,
            files_failed: 0,
            rows_processed: 0,
            rows_uploaded: 0,
            rows_filtered: 0,
            data_processed_mb: 0.0,
        },
    };
    
    println!("{} Starting data upload process...", "🚀".cyan().bold());

    // Verify existing data
    println!("{} Verifying existing data in '{}'...", "🔍".cyan().bold(), args.table_name);
    let verify_result = app.verify_upload(&args.table_name).await;

    match verify_result {
        Ok((count, date_range)) => {
            if count > 0 {
                println!("{}", "✅ Verification successful:".green().bold());
                println!("- {} Existing records found.", count.to_string().yellow().bold());
                if let Some((min_date, max_date)) = date_range {
                    println!("- Data range from {} to {}.", min_date.to_string().yellow().bold(), max_date.to_string().yellow().bold());
                }
            } else {
                println!("{}", "✅ Table is empty. Ready for new data.".green().bold());
            }
        },
        Err(e) => {
            println!("{}", "❌ Verification failed:".red().bold());
            return Err(e.context(format!("Failed to verify table '{}'", args.table_name)));
        }
    }

    // Process files
    println!("{} Processing files from directory '{}'...", "📂".cyan().bold(), args.data_path);
    let total_rows_uploaded = app.process_directory(&args.data_path, &args.table_name).await?;
    
    let total_time_elapsed = app.stats.start_time.elapsed().as_secs_f64();
    let rows_per_second = (total_rows_uploaded as f64) / total_time_elapsed;

    println!("{}", "\n🎉 Data upload complete!".green().bold());
    println!("- {} files processed.", app.stats.files_processed.to_string().yellow().bold());
    println!("- {} rows uploaded in {:.2} seconds.", total_rows_uploaded.to_string().yellow().bold(), total_time_elapsed);
    println!("- Upload speed: {:.2} rows/sec.", rows_per_second.to_string().yellow().bold());

    Ok(())
}

impl AppContext {
    async fn process_directory(&self, data_path: &str, table_name: &str) -> Result<u64> {
        let mut total_uploaded_rows = 0;
        let entries = walkdir::WalkDir::new(data_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && e.path().extension().and_then(|s| s.to_str()) == Some("parquet"));

        for entry in entries {
            let path = entry.path();
            let file_size_mb = path.metadata()?.len() as f64 / (1024.0 * 1024.0);
            
            println!("- {} Reading file: {}", "➡️".blue(), path.display().to_string().yellow());

            let mut file = std::fs::File::open(path)?;
            let reader = ParquetReader::new(&mut file);
            let df = reader.finish()?;
            
            let batch = self.transform_and_filter(&df, table_name)?;
            
            if !batch.is_empty() {
                self.insert_batch(table_name, batch).await?;
                total_uploaded_rows += df.height() as u64;
                println!("- {} Uploaded {} rows.", "✅".green(), df.height().to_string().yellow());
            } else {
                println!("- {} No rows to upload from this file.", "⚠️".yellow());
            }
        }
        
        Ok(total_uploaded_rows)
    }

    fn transform_and_filter<T: Row + Send + 'static + Serialize>(&self, df: &DataFrame, table_name: &str) -> Result<Vec<T>> {
        let rows = df.to_rows()?;
        
        let batch: Vec<T> = rows
            .into_iter()
            .map(|row| self.to_taxi_trip(&row, table_name))
            .filter_map(Result::ok)
            .collect();
        
        Ok(batch)
    }

    async fn insert_batch<T: Row + Send + 'static + serde::Serialize>(&self, table_name: &str, batch: Vec<T>) -> Result<()> {
        let mut insert = self.client.insert(table_name)?;
        
        for row in batch {
            insert.write(&row).await?;
        }
        
        insert.end().await?;
        Ok(())
    }
    
    async fn verify_upload(&self, table_name: &str) -> Result<(u64, Option<(NaiveDate, NaiveDate)>)> {
        let count_query = format!("SELECT COUNT(*) FROM {}", table_name);
        let mut cursor = self.client.query(&count_query).fetch::<u64>()?;
        let count = cursor.next().await?.unwrap_or(0);

        let date_col = "pickup_date";
        let range_query = format!("SELECT MIN({}), MAX({}) FROM {}", date_col, date_col, table_name);
        let mut cursor = self.client.query(&range_query).fetch::<(String, String)>()?;
        
        let date_range = match cursor.next().await? {
            Some((min_str, max_str)) => {
                let min = NaiveDate::parse_from_str(&min_str, "%Y-%m-%d").ok();
                let max = NaiveDate::parse_from_str(&max_str, "%Y-%m-%d").ok();
                if let (Some(min), Some(max)) = (min, max) {
                    Some((min, max))
                } else {
                    None
                }
            },
            None => None,
        };
        
        Ok((count, date_range))
    }

    fn to_taxi_trip<T: Row + Send + 'static>(&self, row: &Vec<String>, table_name: &str) -> Result<T> {
        let result: Result<T> = if table_name == "yellow_taxi_trips" {
            let record = YellowTaxiTrip::try_from(row)
                .context("Failed to convert row to YellowTaxiTrip")?;
            Ok(unsafe { std::mem::transmute(record) })
        } else if table_name == "green_taxi_trips" {
            let record = GreenTaxiTrip::try_from(row)
                .context("Failed to convert row to GreenTaxiTrip")?;
            Ok(unsafe { std::mem::transmute(record) })
        } else {
            Err(anyhow::anyhow!("Unknown table type: {}", table_name))
        };
        
        result
    }
}