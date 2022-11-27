use std::path::Path;
use std::time::Duration;

use approx::assert_abs_diff_eq;
use lazy_static::lazy_static;
use serde::Deserialize;
use tempfile::tempdir;

use crate::engine::MetricsEngine;
use crate::engine::io::{AddCountValue, AddGaugeValue};
use crate::engine::querying::{MetricQuery, MetricQueryExpression};
use crate::metric::common::{GenericMetric, MetricType, MetricConfig, MetricStorageDurationConfig};
use crate::metric::common::CountInput;
use crate::metric::count::DefaultCountMetric;
use crate::metric::expression::{ArithmeticOperation, CompareOperation, FilterExpression, Function, TransformExpression};
use crate::metric::gauge::DefaultGaugeMetric;
use crate::metric::OperationResult;
use crate::metric::ratio::{DefaultRatioMetric, RatioInput};
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{GroupKey, GroupValue, Query, TimeRange};

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
        Some(0.667791913241474),
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
        Some(0.6676946286179699),
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
        Some(0.6676941904100635),
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
        Some(0.8143444467992389),
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
            (1654596000.0, Some(0.7810616929429516)),
            (1654596500.0, Some(0.7984125255297858)),
            (1654597000.0, Some(0.7794862563100798)),
            (1654597500.0, Some(0.7288043881887649)),
            (1654598000.0, Some(0.6590226288448248)),
            (1654598500.0, Some(0.5867395178973674)),
            (1654599000.0, Some(0.5302283972157132)),
            (1654599500.0, Some(0.5026470303535462)),
            (1654600000.0, Some(0.5116417237243929)),
            (1654600500.0, Some(0.554182909562916)),
            (1654601000.0, Some(0.6203749242122579)),
            (1654601500.0, Some(0.693674767847678)),
            (1654602000.0, Some(0.7563903743624687)),
            (1654602500.0, Some(0.7929651886075586))
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
        Some(0.8006096125819039),
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
            (GroupValue::from_ref("T1"), Some(0.6676941904100635)),
            (GroupValue::from_ref("T2"), Some(0.6676950667588899))
        ]),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_group_by(GroupKey::from_ref("tag"))
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
            (GroupValue::from_ref("T1"), Some(0.6677078367421156)),
            (GroupValue::from_ref("T2"), Some(0.6676991150199966))
        ]),
        metric.average(
            Query::new(TimeRange::new(start_time, end_time))
                .with_group_by(GroupKey::from_ref("tag"))
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
        Some(0.667791913241474),
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
        Some(0.6677034751310084),
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
        Some(0.6677034751310084),
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
        Some(0.6677034751310084),
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
        0.8004917040615059,
        metric.percentile(Query::new(TimeRange::new(start_time, end_time)), 95).value().unwrap_or(0.0),
        epsilon = 1e-5
    );
}

#[test]
fn test_gauge_segments_reload1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    config.durations[0].segment_duration = 6.0 * 60.0 * 60.0;
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    let mut count = 0;
    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();

        count += 1;
        if count == 2000000 {
            metric = DefaultGaugeMetric::from_existing(temp_metric_data.path()).unwrap();
        }

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.667791913241474),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_segments_remove1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    config.durations[0].max_segments = Some(30);
    config.durations[0].segment_duration = 6.0 * 3600.0;
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();
        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.667791913241474),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_segments_remove2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    config.durations[0].max_segments = Some(20);
    config.durations[0].segment_duration = 6.0 * 3600.0;
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();
        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.667791913241474),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_multiple_durations1() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    let mut faster_duration = MetricStorageDurationConfig::default_for(MetricType::Gauge);
    faster_duration.datapoint_duration = 0.1;
    config.durations.push(faster_duration);
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();
        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.667791913241474),
        metric.average(Query::new(TimeRange::new(start_time, end_time))).value()
    );
}

#[test]
fn test_gauge_multiple_durations2() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    config.durations[0].datapoint_duration = 10.0;
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();
        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![
            (1654596000.0, Some(0.7623410224914551)),
            (1654596015.0, Some(0.7596292793750763)),
            (1654596030.0, Some(0.7643710076808929)),
            (1654596045.0, Some(0.7709805965423584))
        ]),
        metric.average_in_window(Query::new(TimeRange::new(start_time, start_time + 60.0)), Duration::from_secs_f64(15.0)).time_values()
    );

    assert_eq!(
        Some(vec![
            (1654596006.0, Some(0.7623410224914551)),
            (1654596016.0, Some(0.7514280676841736)),
            (1654596026.0, Some(0.767830491065979)),
            (1654596036.0, Some(0.7641525864601135)),
            (1654596044.0, Some(0.7645894289016724)),
            (1654596054.0, Some(0.7709805965423584))
        ]),
        metric.average_in_window(Query::new(TimeRange::new(start_time, start_time + 60.0)), Duration::from_secs_f64(2.0)).time_values()
    );
}

#[test]
fn test_gauge_multiple_durations3() {
    let temp_metric_data = tempdir().unwrap();

    let start_time = 1654077600.0 + 6.0 * 24.0 * 3600.0;
    let end_time = start_time + 2.0 * 3600.0;

    let mut config = MetricConfig::new(MetricType::Gauge);
    config.durations[0].datapoint_duration = 10.0;
    let mut faster_duration = MetricStorageDurationConfig::default_for(MetricType::Gauge);
    faster_duration.datapoint_duration = 1.0;
    config.durations.push(faster_duration);
    let mut metric = DefaultGaugeMetric::with_config(temp_metric_data.path(), config).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        metric.add(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, Vec::new()).unwrap();
        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![
            (1654596000.0, Some(0.7623410224914551)),
            (1654596015.0, Some(0.7596292793750763)),
            (1654596030.0, Some(0.7643710076808929)),
            (1654596045.0, Some(0.7709805965423584))
        ]),
        metric.average_in_window(Query::new(TimeRange::new(start_time, start_time + 60.0)), Duration::from_secs_f64(15.0)).time_values()
    );

    assert_eq!(
        Some(vec![
            (1654596000.0, Some(0.7637167572975159)),
            (1654596002.0, Some(0.7580450773239136)),
            (1654596004.0, Some(0.7613156139850616)),
            (1654596006.0, Some(0.7570991516113281)),
            (1654596008.0, Some(0.7641935348510742)),
            (1654596010.0, Some(0.7648496627807617)),
            (1654596012.0, Some(0.7661760151386261)),
            (1654596014.0, Some(0.7678832113742828)),
            (1654596016.0, Some(0.760563462972641)),
            (1654596018.0, Some(0.7685678601264954)),
            (1654596020.0, Some(0.7670826613903046)),
            (1654596022.0, Some(0.7649800479412079)),
            (1654596024.0, Some(0.7651969194412231)),
            (1654596026.0, Some(0.7659425735473633)),
            (1654596028.0, Some(0.7556097507476807)),
            (1654596030.0, Some(0.761206328868866)),
            (1654596032.0, Some(0.7634029388427734)),
            (1654596034.0, Some(0.7706423401832581)),
            (1654596036.0, Some(0.7674989998340607)),
            (1654596038.0, Some(0.758998692035675)),
            (1654596040.0, Some(0.7695607244968414)),
            (1654596042.0, Some(0.7605744898319244)),
            (1654596044.0, Some(0.7630680203437805)),
            (1654596046.0, Some(0.7692327797412872)),
            (1654596048.0, Some(0.7624682188034058)),
            (1654596050.0, Some(0.7596468925476074)),
            (1654596052.0, Some(0.7719657719135284)),
            (1654596054.0, Some(0.7644509077072144)),
            (1654596056.0, Some(0.76540207862854)),
            (1654596058.0, Some(0.7614836096763611))
        ]),
        metric.average_in_window(Query::new(TimeRange::new(start_time, start_time + 60.0)), Duration::from_secs_f64(2.0)).time_values()
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
        Some(144322.0),
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
        Some(144338.0),
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
        Some(144338.0),
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
        Some(0.46893058577347874),
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
                (1654601500.0, Some(0.41670732897433393)),
                (1654602500.0, Some(1.0))
            ]
        ),
        metric.sum_in_window(
            Query::new(TimeRange::new(start_time, end_time))
                .with_output_filter(
                    FilterExpression::Compare {
                        operation: CompareOperation::GreaterThan,
                        left: Box::new(FilterExpression::Value(TransformExpression::InputDenominator)),
                        right: Box::new(FilterExpression::Value(TransformExpression::Value(10000.0)))
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
        Some(0.46898945530629493),
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
    metrics_engine.add_metric("cpu", MetricType::Gauge).unwrap();
    metrics_engine.add_metric("perf_events", MetricType::Count).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.count("perf_events", [AddCountValue::new(SAMPLE_DATA.times[index], CountInput(1), tags)].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(0.6676946286179699),
        metrics_engine.average("cpu", Query::new(TimeRange::new(start_time, end_time))).unwrap().value()
    );

    assert_eq!(
        Some(144338.0),
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
    metrics_engine.add_metric("cpu", MetricType::Gauge).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(100.0 * 0.6676946286179699),
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
    metrics_engine.add_metric("cpu1", MetricType::Gauge).unwrap();
    metrics_engine.add_metric("cpu2", MetricType::Gauge).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(1.4583610857319427),
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
    metrics_engine.add_metric("cpu1", MetricType::Gauge).unwrap();
    metrics_engine.add_metric("cpu2", MetricType::Gauge).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![(GroupValue::from_ref("1"), Some(1.4584017863792649)), (GroupValue::from_ref("2"), Some(1.458320393630842))]),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Divide,
                    left: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu1".to_string(),
                            query: Query::placeholder().with_group_by(GroupKey::from_ref("core"))
                        }
                    ),
                    right: Box::new(
                        MetricQueryExpression::Average {
                            metric: "cpu2".to_string(),
                            query: Query::placeholder().with_group_by(GroupKey::from_ref("core"))
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
    metrics_engine.add_metric("cpu1", MetricType::Gauge).unwrap();
    metrics_engine.add_metric("cpu2", MetricType::Gauge).unwrap();

    for index in 0..SAMPLE_DATA.times.len() {
        let tags = vec![tags_list[(index % 2)].to_owned()];
        metrics_engine.gauge("cpu1", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();
        metrics_engine.gauge("cpu2", [AddGaugeValue::new(SAMPLE_DATA.times[index], SAMPLE_DATA.values[index] as f64 * SAMPLE_DATA.values[index] as f64, tags.clone())].into_iter()).unwrap();

        if SAMPLE_DATA.times[index] >= end_time + 3600.0 {
            break;
        }
    }

    assert_eq!(
        Some(vec![(GroupValue::from_ref("1"), Some(0.6676941904100635)), (GroupValue::from_ref("2"), Some(0.6676950667588899))]),
        metrics_engine.query(
            MetricQuery::new(
                TimeRange::new(start_time, end_time),
                MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average {
                            metric: "cpu1".to_string(),
                            query: Query::placeholder().with_group_by(GroupKey::from_ref("core"))
                        },
                        MetricQueryExpression::Average {
                            metric: "cpu2".to_string(),
                            query: Query::placeholder().with_group_by(GroupKey::from_ref("core"))
                        }
                    ]
                }
            )
        ).unwrap().group_values()
    );
}