use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::metric::{DefaultMetric};
use crate::helpers::{TimeMeasurement, TimeMeasurementUnit};
use crate::model::{Query, Tags, TimeRange};
use crate::operations::TransformOperation;
use crate::tags::TagsFilter;

mod helpers;
mod memory_file;
mod storage;
mod metric;
mod metric_operations;
mod operations;
mod model;
mod tags;

#[derive(Deserialize)]
struct SampleData {
    times: Vec<f64>,
    values: Vec<f32>
}

fn main() {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();
    let tags_list = vec!["tag:T1", "tag:T2"];

    println!("n: {}", data.times.len());

    let mut metric = DefaultMetric::new(Path::new("metrics"));

    {
        let _m = TimeMeasurement::new("gauge", TimeMeasurementUnit::Seconds);
        for index in 0..data.times.len() {
            // let tags = &[&tags_list[0]];
            let tags = &[tags_list[(index % 2)]];
            metric.gauge(data.times[index], data.values[index] as f64, tags).unwrap();
        }
    }

    metric.stats();

    // let mut metric = DefaultMetric::from_existing(Path::new("metrics"));

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    // Avg: 0.6676723153748684
    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", metric.average(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", metric.average(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg (tags=1): {}", metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::And(metric.tags_pattern(&[&tags_list[0]]).unwrap()))
        ).unwrap_or(0.0));
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg sqrt: {}", metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_input_transform(TransformOperation::Sqrt)
        ).unwrap());
    }

    {
        let _m = TimeMeasurement::new("max", TimeMeasurementUnit::Microseconds);
        println!("Max: {}", metric.max(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("95th", TimeMeasurementUnit::Microseconds);
        println!("95th: {}", metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average_in_window", TimeMeasurementUnit::Microseconds);

        let windows = metric.average_in_window(Query::new(TimeRange::new(start_time, end_time)), Duration::from_secs_f64(30.0));
        // let windows = metric.percentile_in_window(Query::new(TimeRange::new(start_time, end_time)), Duration::from_secs_f64(30.0), 95);
        std::fs::write(
            &Path::new("window.json"),
            serde_json::to_string(&windows).unwrap()
        ).unwrap();
    }
}
