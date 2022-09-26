use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::database::{DefaultDatabase};
use crate::helpers::{TimeMeasurement, TimeMeasurementUnit};
use crate::model::{Query, Tags, TimeRange};
use crate::operations::TransformOperation;

mod helpers;
mod memory_file;
mod storage;
mod database;
mod operations;
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
            let tags: Tags = 0;
            // let tags = (index % 2) as Tags;
            database.gauge(data.times[index], data.values[index] as f64, tags);
        }
    }

    database.stats();

    // let mut database = DefaultDatabase::from_existing(Path::new("metrics"));

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    // Avg: 0.6676723153748684
    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", database.average(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", database.average(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg sqrt: {}", database.average(Query::with_transform(TimeRange::new(start_time, end_time), TransformOperation::Sqrt)).unwrap());
    }

    {
        let _m = TimeMeasurement::new("max", TimeMeasurementUnit::Microseconds);
        println!("Max: {}", database.max(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("95th", TimeMeasurementUnit::Microseconds);
        println!("95th: {}", database.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average_in_window", TimeMeasurementUnit::Microseconds);

        let windows = database.average_in_window(Query::new(TimeRange::new(start_time, end_time)), Duration::from_secs_f64(30.0));
        std::fs::write(
            &Path::new("window.json"),
            serde_json::to_string(&windows).unwrap()
        ).unwrap();
    }
}
