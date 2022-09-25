use std::path::Path;

use serde::Deserialize;

use crate::database::{DefaultDatabase};
use crate::helpers::{TimeMeasurement, TimeMeasurementUnit};
use crate::model::{Tags, TimeRange};

mod helpers;
mod memory_file;
mod storage;
mod database;
mod algorithms;
mod model;

#[derive(Deserialize)]
struct SampleData {
    times: Vec<f64>,
    values: Vec<f32>
}

fn main() {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();

    println!("n: {}", data.times.len());

    let mut database = DefaultDatabase::new(Path::new("metrics"));
    {
        let _m = TimeMeasurement::new("gauge", TimeMeasurementUnit::Seconds);
        for index in 0..data.times.len() {
            // let tags: Tags = 0;
            let tags = (index % 2) as Tags;
            database.gauge(data.times[index], data.values[index] as f64, tags);
        }
    }

    database.stats();

    // let mut database = DefaultDatabase::from_existing(Path::new("metrics"));

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    // Avg: 0.6676723153748684
    {
        let _m = TimeMeasurement::new("average_linear_scan", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", database.average(TimeRange::new(start_time, end_time), false));
    }

    {
        let _m = TimeMeasurement::new("average_linear_scan", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", database.average(TimeRange::new(start_time, end_time), false));
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", database.average(TimeRange::new(start_time, end_time), true));
    }

    {
        let _m = TimeMeasurement::new("Max", TimeMeasurementUnit::Microseconds);
        println!("Max: {}", database.max(TimeRange::new(start_time, end_time), true));
    }

    {
        let _m = TimeMeasurement::new("95th", TimeMeasurementUnit::Microseconds);
        println!("95th: {}", database.percentile(TimeRange::new(start_time, end_time), true, 95).unwrap());
    }
}
