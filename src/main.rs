use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use crate::engine::MetricsEngine;

use crate::helpers::{TimeMeasurement, TimeMeasurementUnit};
use crate::model::{Query, Tags, TimeRange};
use crate::metric::count::DefaultCountMetric;
use crate::metric::gauge::DefaultGaugeMetric;
use crate::metric::operations::TransformOperation;
use crate::tags::{PrimaryTag, TagsFilter};

mod helpers;
mod storage;
mod model;
mod tags;
mod metric;
mod engine;

#[cfg(test)]
mod integration_tests;

#[derive(Deserialize)]
struct SampleData {
    times: Vec<f64>,
    values: Vec<f32>
}

fn main_gauge() {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();
    let tags_list = vec!["tag:T1", "tag:T2"];

    println!("n: {}", data.times.len());

    let mut metric = DefaultGaugeMetric::new(Path::new("test_metric")).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    {
        let _m = TimeMeasurement::new("gauge", TimeMeasurementUnit::Seconds);
        for index in 0..data.times.len() {
            // let tags = &[];
            let tags = &[tags_list[(index % 2)]];
            metric.add(data.times[index], data.values[index] as f64, tags).unwrap();
        }
    }

    metric.stats();

    // let mut metric = DefaultGaugeMetric::from_existing(Path::new("test_metric")).unwrap();

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
        println!("Avg (tags=0,1): {}", metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].to_string(), tags_list[1].to_string()]))
        ).unwrap_or(0.0));
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg (tags=0): {}", metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::And(vec![tags_list[0].to_string()]))
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

fn main_count() {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();
    let tags_list = vec!["tag:T1", "tag:T2"];

    println!("n: {}", data.times.len());

    let mut metric = DefaultCountMetric::new(Path::new("test_metric")).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    {
        let _m = TimeMeasurement::new("count", TimeMeasurementUnit::Seconds);
        for index in 0..data.times.len() {
            // let tags = &[];
            let tags = &[tags_list[(index % 2)]];
            metric.add(data.times[index], 1, tags).unwrap();
        }
    }

    metric.stats();

    // let mut metric = DefaultCountMetric::from_existing(Path::new("test_metric")).unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    {
        let _m = TimeMeasurement::new("sum", TimeMeasurementUnit::Microseconds);
        println!("Sum: {}", metric.sum(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average", TimeMeasurementUnit::Microseconds);
        println!("Avg: {}", metric.average(Query::new(TimeRange::new(start_time, end_time))).unwrap());
    }

    {
        let _m = TimeMeasurement::new("sum", TimeMeasurementUnit::Microseconds);
        println!("Sum (tags=0,1): {}", metric.sum(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].to_string(), tags_list[1].to_string()]))
        ).unwrap_or(0.0));
    }

    {
        let _m = TimeMeasurement::new("sum", TimeMeasurementUnit::Microseconds);
        println!("Sum (tags=0): {}", metric.sum(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::And(vec![tags_list[0].to_string()]))
        ).unwrap_or(0.0));
    }

    {
        let _m = TimeMeasurement::new("sum", TimeMeasurementUnit::Microseconds);
        println!("Sum sqrt: {}", metric.sum(
            Query::new(TimeRange::new(start_time, end_time))
                .with_output_transform(TransformOperation::Sqrt)
        ).unwrap());
    }

    {
        let _m = TimeMeasurement::new("average_in_window", TimeMeasurementUnit::Microseconds);

        let windows = metric.sum_in_window(Query::new(TimeRange::new(start_time, end_time)), Duration::from_secs_f64(30.0));
        // let windows = metric.average_in_window(Query::new(TimeRange::new(start_time, end_time)), Duration::from_secs_f64(30.0));
        std::fs::write(
            &Path::new("window.json"),
            serde_json::to_string(&windows).unwrap()
        ).unwrap();
    }
}

fn main_engine() {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();
    let tags_list = vec!["tag:T1", "tag:T2"];

    println!("n: {}", data.times.len());

    let mut metrics_engine = MetricsEngine::new(&Path::new("test_metric_engine")).unwrap();
    metrics_engine.add_gauge_metric("cpu").unwrap();
    metrics_engine.add_count_metric("perf_events").unwrap();

    {
        let _m = TimeMeasurement::new("gauge & count", TimeMeasurementUnit::Seconds);
        for index in 0..data.times.len() {
            let tags = &[tags_list[(index % 2)]];
            metrics_engine.gauge("cpu", data.times[index], data.values[index] as f64, tags).unwrap();
            metrics_engine.count("perf_events", data.times[index], 1, tags).unwrap();
        }
    }

    // let mut metrics_engine = MetricsEngine::from_existing(&Path::new("test_metric_engine")).unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    println!("Avg: {}", metrics_engine.average("cpu", Query::new(TimeRange::new(start_time, end_time))).unwrap());
    println!("Count: {}", metrics_engine.sum("perf_events", Query::new(TimeRange::new(start_time, end_time))).unwrap());
}

fn main() {
    main_gauge();
    // main_count();
    // main_engine();
}