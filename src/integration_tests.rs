use std::path::Path;
use std::time::Duration;

use approx::assert_abs_diff_eq;
use lazy_static::lazy_static;
use serde::Deserialize;
use tempfile::tempdir;

use crate::engine::MetricsEngine;
use crate::engine::io::{AddCountValue, AddGaugeValue};
use crate::engine::querying::{MetricQuery, MetricQueryExpression};
use crate::metric::common::GenericMetric;
use crate::metric::common::CountInput;
use crate::metric::count::DefaultCountMetric;
use crate::metric::expression::{ArithmeticOperation, CompareOperation, FilterExpression, Function, TransformExpression};
use crate::metric::gauge::DefaultGaugeMetric;
use crate::metric::OperationResult;
use crate::metric::ratio::{DefaultRatioMetric, RatioInput};
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{Query, TimeRange};

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
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_average2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_average3() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676758207088794),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::And(vec![tags_list[0].clone()]))
        ).value()
    );
}

#[test]
fn test_gauge_average4() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.814266989356846),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_input_transform(TransformExpression::Function { function: Function::Sqrt, arguments: vec![TransformExpression::InputValue] })
        ).value()
    );
}

#[test]
fn test_gauge_average5() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![
            (1654596000.0, Some(0.7810451562759174)),
            (1654596500.0, Some(0.798498850655144)),
            (1654597000.0, Some(0.7795522142153012)),
            (1654597500.0, Some(0.7289274156650085)),
            (1654598000.0, Some(0.6592202344433357)),
            (1654598500.0, Some(0.5868039118632926)),
            (1654599000.0, Some(0.5303680305548479)),
            (1654599500.0, Some(0.5027029820642929)),
            (1654600000.0, Some(0.5116486969877181)),
            (1654600500.0, Some(0.5541196308760059)),
            (1654601000.0, Some(0.6199090911265585)),
            (1654601500.0, Some(0.6936228376297285)),
            (1654602000.0, Some(0.7563260559193605)),
            (1654602500.0, Some(0.7929676963694573))
        ]),
        metric.average_in_window(
            Query::new(TimeRange::new(start_time, end_time)),
            Duration::from_secs_f64(500.0)
        ).time_values()
    );
}

#[test]
fn test_gauge_max1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.8197603225708008),
        metric.max(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_95th1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.8006128556989215),
        metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).value()
    );
}

#[test]
fn test_gauge_group_by_average1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        OperationResult::GroupValues(vec![
            ("T1".to_owned(), Some(0.6676758207088794)),
            ("T2".to_owned(), Some(0.6676688100408572))
        ]),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_group_by("tag".to_owned())
        )
    );
}

#[test]
fn test_gauge_group_by_average2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        OperationResult::GroupValues(vec![
            ("T1".to_owned(), Some(0.6676758207088794)),
            ("T2".to_owned(), Some(0.6676688100408572))
        ]),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_group_by("tag".to_owned())
        )
    );
}

#[test]
fn test_gauge_reload1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();

    let mut count = 0;
    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        count += 1;
        if count == 6000 {
            metric = DefaultGaugeMetric::from_existing(temp_metric_data.path()).unwrap();
        }

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_primary_tag_average1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_primary_tag_average2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].clone(), tags_list[1].clone()]))
        ).value()
    );
}

#[test]
fn test_gauge_auto_primary_tag_average1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_auto_primary_tag("tag").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    let mut primary_tags = metric.primary_tags().collect::<Vec<_>>();
    primary_tags.sort();
    assert_eq!(
        vec![&PrimaryTag::Default, &PrimaryTag::Named(tags_list[0].clone()), &PrimaryTag::Named(tags_list[1].clone())],
        primary_tags
    );

    assert_eq!(
        Some(0.6676723153748684),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].clone(), tags_list[1].clone()]))
        ).value()
    );
}

#[test]
fn test_gauge_primary_tag_95th1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultGaugeMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_abs_diff_eq!(
        0.8006199979022869,
        metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).value().unwrap_or(0.0),
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
        metric.add(SAMPLE_DATA.times[index], CountInput(1), Vec::new()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(144328.0),
        metric.sum(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_count_primary_tag_sum1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultCountMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], CountInput(1), tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(144328.0),
        metric.sum(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_count_primary_tag_sum2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultCountMetric::new(temp_metric_data.path()).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(SAMPLE_DATA.times[index], CountInput(1), tags).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(144328.0),
        metric.sum(
            Query::new(TimeRange::new(start_time, end_time))
                .with_tags_filter(TagsFilter::Or(vec![tags_list[0].clone(), tags_list[1].clone()]))
        ).value()
    );
}

#[test]
fn test_ratio_sum1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultRatioMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(
            SAMPLE_DATA.times[index],
            RatioInput(CountInput(if SAMPLE_DATA.values[index] > 0.7 {1} else {0}), CountInput(1)),
            Vec::new()
        ).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.4689526633778615),
        metric.sum(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_ratio_sum2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut metric = DefaultRatioMetric::new(temp_metric_data.path()).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(
            SAMPLE_DATA.times[index],
            RatioInput(CountInput(if SAMPLE_DATA.values[index] > 0.7 {1} else {0}), CountInput(1)),
            Vec::new()
        ).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(
            vec![
                (1654596500.0, Some(1.0)),
                (1654597000.0, Some(1.0)),
                (1654598500.0, Some(0.0)),
                (1654599500.0, Some(0.0)),
                (1654600000.0, Some(0.0)),
                (1654600500.0, Some(0.0)),
                (1654601500.0, Some(0.415959418593308)),
                (1654602500.0, Some(1.0))
            ]
        ),
        metric.sum_in_window(
            Query::new(TimeRange::new(start_time, end_time))
                .with_output_filter(
                    FilterExpression::Compare {
                        operation: CompareOperation::GreaterThan,
                        left: Box::new(FilterExpression::Transform(TransformExpression::InputDenominator)),
                        right: Box::new(FilterExpression::Transform(TransformExpression::Value(10000.0)))
                    }
                )
            ,
            Duration::from_secs_f64(500.0),
        ).time_values()
    );
}

#[test]
fn test_ratio_primary_tag_sum1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let mut metric = DefaultRatioMetric::new(temp_metric_data.path()).unwrap();

    metric.add_primary_tag(PrimaryTag::Named(tags_list[0].clone())).unwrap();
    metric.add_primary_tag(PrimaryTag::Named(tags_list[1].clone())).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metric.add(
            SAMPLE_DATA.times[index],
            RatioInput(CountInput(if SAMPLE_DATA.values[index] > 0.7 {1} else {0}), CountInput(1)),
            tags
        ).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.4689526633778615),
        metric.sum(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_metrics_engine1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let metrics_engine = MetricsEngine::new(&Path::new(temp_metric_data.path())).unwrap();
    metrics_engine.add_gauge_metric("cpu").unwrap();
    metrics_engine.add_count_metric("perf_events").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.count("perf_events", [AddCountValue::new(SAMPLE_DATA.times[index], CountInput(1), tags)].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676723153748684),
        metrics_engine.average("cpu", Query::new(TimeRange::new(start_time, end_time))).unwrap().value()
    );

    assert_eq!(
        Some(144328.0),
        metrics_engine.sum("perf_events", Query::new(TimeRange::new(start_time, end_time))).unwrap().value()
    );
}

#[test]
fn test_metrics_engine_query1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let metrics_engine = MetricsEngine::new(&Path::new(temp_metric_data.path())).unwrap();
    metrics_engine.add_gauge_metric("cpu").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(100.0 * 0.6676723153748684),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Multiply,
                    left: Box::new(MetricQueryExpression::Value(100.0)),
                    right: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu".to_string(),
                            query: Query::placeholder()
                        }
                    )
                }
            )
        ).unwrap().value()
    );
}

#[test]
fn test_metrics_engine_query2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")];

    let metrics_engine = MetricsEngine::new(&Path::new(temp_metric_data.path())).unwrap();
    metrics_engine.add_gauge_metric("cpu1").unwrap();
    metrics_engine.add_gauge_metric("cpu2").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(1.45836899750842),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Divide,
                    left: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu1".to_string(),
                            query: Query::placeholder()
                        }
                    ),
                    right: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu2".to_string(),
                            query: Query::placeholder()
                        }
                    )
                }
            )
        ).unwrap().value()
    );
}

#[test]
fn test_metrics_engine_query3() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("core", "1"), Tag::from_ref("core", "2")];

    let metrics_engine = MetricsEngine::new(&Path::new(temp_metric_data.path())).unwrap();
    metrics_engine.add_gauge_metric("cpu1").unwrap();
    metrics_engine.add_gauge_metric("cpu2").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![("1".to_owned(), Some(1.458353977970301)), ("2".to_owned(), Some(1.458384017513627))]),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Divide,
                    left: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu1".to_string(),
                            query: Query::placeholder().with_group_by("core".to_owned())
                        }
                    ),
                    right: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu2".to_string(),
                            query: Query::placeholder().with_group_by("core".to_owned())
                        }
                    )
                }
            )
        ).unwrap().group_values()
    );
}

#[test]
fn test_metrics_engine_query4() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;
    let tags_list = vec![Tag::from_ref("core", "1"), Tag::from_ref("core", "2")];

    let metrics_engine = MetricsEngine::new(&Path::new(temp_metric_data.path())).unwrap();
    metrics_engine.add_gauge_metric("cpu1").unwrap();
    metrics_engine.add_gauge_metric("cpu2").unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![("1".to_owned(), Some(0.6676758207088794)), ("2".to_owned(), Some(0.6676688100408572))]),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average {
                            metric: "cpu1".to_string(),
                            query: Query::placeholder().with_group_by("core".to_owned())
                        },
                        MetricQueryExpression::Average {
                            metric: "cpu2".to_string(),
                            query: Query::placeholder().with_group_by("core".to_owned())
                        }
                    ]
                }
            )
        ).unwrap().group_values()
    );
}