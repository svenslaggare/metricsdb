use approx::assert_abs_diff_eq;
use lazy_static::lazy_static;
use serde::Deserialize;
use tempfile::tempdir;

use crate::{DefaultCountMetric, DefaultGaugeMetric, PrimaryTag, Query, TagsFilter, TimeRange, TransformOperation};

#[derive(Deserialize)]
struct SampleData {
    times: Vec<f64>,
    values: Vec<f32>
}

lazy_static! {
    static ref SAMPLE_DATA: SampleData = load_sample_data();
}

fn load_sample_data() -> SampleData {
    let data = std::fs::read_to_string("output.json").unwrap();
    let data: SampleData = serde_json::from_str(&data).unwrap();
    data
}

#[test]
fn test_gauge_average1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, &[]).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(0.6676723153748684), metric.average(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_gauge_average2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(0.6676723153748684), metric.average(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_gauge_average3() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676758207088794),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::And(vec![tags_list[0].to_string()]))
        )
    );
}

#[test]
fn test_gauge_average4() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, &[]).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.814266989356846),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_input_transform(TransformOperation::Sqrt)
        )
    );
}

#[test]
fn test_gauge_max1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, &[]).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(0.8197603225708008), metric.max(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_gauge_95th1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, &[]).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(0.8005562248849434), metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95));
}

#[test]
fn test_gauge_primary_tag_average1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(0.6676723153748684), metric.average(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_gauge_primary_tag_average2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].to_string(), tags_list[1].to_string()]))
        )
    );
}

#[test]
fn test_gauge_primary_tag_95th1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_abs_diff_eq!(
        0.8006655905325116,
        metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).unwrap_or(0.0),
        epsilon = 1e-5
    );
}

#[test]
fn test_count_sum1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultCountMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], 1, &[]).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(144328.0), metric.sum(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_count_primary_tag_sum1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultCountMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], 1, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(Some(144328.0), metric.sum(Query::new(TimeRange::new(start_time, end_time))));
}

#[test]
fn test_count_primary_tag_sum2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec!["tag:T1", "tag:T2"];

    let mut metric = DefaultCountMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T1".to_owned())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named("tag:T2".to_owned())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = &[tags_list[(index % 2)]];
        metric.add(SAMPLE_DATA.times[index], 1, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(144328.0),
        metric.sum(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].to_string(), tags_list[1].to_string()]))
        )
    );
}