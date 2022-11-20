use std::collections::HashMap;
use std::time::Duration;
use fnv::{FnvHashMap, FnvHashSet};

use serde::Deserialize;

use crate::engine::engine::MetricsEngine;
use crate::engine::io::{MetricsEngineError, MetricsEngineResult};
use crate::metric::{GroupTimeValues, GroupValues, OperationResult, TimeValues};
use crate::metric::expression::{ArithmeticOperation, Function};
use crate::model::{Query, TimeRange};

pub struct MetricQuery {
    pub time_range: TimeRange,
    pub expression: MetricQueryExpression
}

impl MetricQuery {
    pub fn new(time_range: TimeRange, expression: MetricQueryExpression) -> MetricQuery {
        MetricQuery {
            time_range,
            expression
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum MetricQueryExpression {
    Average { metric: String, query: Query },
    Sum { metric: String, query: Query },
    Max { metric: String, query: Query },
    Min { metric: String, query: Query },
    Percentile { metric: String,  query: Query, percentile: i32 },
    Value(f64),
    Arithmetic { operation: ArithmeticOperation, left: Box<MetricQueryExpression>, right: Box<MetricQueryExpression> },
    Function { function: Function, arguments: Vec<MetricQueryExpression> }
}

pub fn query<T: MetricQueryable>(engine: &T, query: MetricQuery) -> MetricsEngineResult<OperationResult> {
    fn evaluate<T: MetricQueryable>(engine: &T, time_range: TimeRange, expression: MetricQueryExpression) -> MetricsEngineResult<OperationResult> {
        match expression {
            MetricQueryExpression::Average { metric, mut query } => {
                query.time_range = time_range;
                engine.average(&metric, query)
            }
            MetricQueryExpression::Sum { metric, mut query } => {
                query.time_range = time_range;
                engine.sum(&metric, query)
            }
            MetricQueryExpression::Max { metric, mut query } => {
                query.time_range = time_range;
                engine.max(&metric, query)
            }
            MetricQueryExpression::Min { metric, mut query } => {
                query.time_range = time_range;
                engine.min(&metric, query)
            }
            MetricQueryExpression::Percentile { metric, mut query, percentile } => {
                query.time_range = time_range;
                engine.percentile(&metric, query, percentile)
            }
            MetricQueryExpression::Value(value) => {
                Ok(OperationResult::Value(Some(value)))
            }
            MetricQueryExpression::Arithmetic { operation, left, right } => {
                let left = evaluate(engine, time_range, *left)?;
                let right = evaluate(engine, time_range, *right)?;

                match (left, right) {
                    (OperationResult::Value(left), OperationResult::GroupValues(right)) => {
                        let transformed_values = right
                            .into_iter()
                            .map(|(right_group, right_value)| (right_group, option_op(left, right_value, |x, y| operation.apply(x, y))))
                            .collect();
                        Ok(OperationResult::GroupValues(sorted_group_values(transformed_values)))
                    }
                    (OperationResult::GroupValues(left), OperationResult::Value(right)) => {
                        let transformed_values = left
                            .into_iter()
                            .map(|(left_group, left_value)| (left_group, option_op(left_value, right, |x, y| operation.apply(x, y))))
                            .collect();
                        Ok(OperationResult::GroupValues(sorted_group_values(transformed_values)))
                    }
                    (OperationResult::GroupValues(left), OperationResult::GroupValues(right)) => {
                        let left = group_map(left);
                        let left_groups = group_keys(&left);
                        let right = group_map(right);
                        let right_groups = group_keys(&right);

                        let transformed_values = left_groups.intersection(&right_groups)
                            .map(|&group| (
                                group.clone(),
                                option_op((&left[group]).clone(), (&right[group]).clone(), |x, y| operation.apply(x, y))
                            ))
                            .collect();
                        Ok(OperationResult::GroupValues(sorted_group_values(transformed_values)))
                    }
                    (OperationResult::Value(left), OperationResult::Value(right)) => {
                        Ok(OperationResult::Value(option_op(left, right, |x, y| operation.apply(x, y))))
                    }
                    _ => { Ok(OperationResult::Value(None)) }
                }
            }
            MetricQueryExpression::Function { function, arguments } => {
                let transformed_arguments = transform_with_result(
                    arguments.into_iter(),
                    |argument| evaluate(engine, time_range, argument)
                )?;

                if transformed_arguments.is_empty() {
                    return Ok(OperationResult::Value(None));
                }

                if transformed_arguments[0].is_group_values() {
                    let transformed_arguments = transform_with_result::<_, _, MetricsEngineError>(
                        transformed_arguments.into_iter(),
                        |argument| Ok(
                            group_map(
                                argument
                                    .group_values()
                                    .ok_or_else(|| MetricsEngineError::UnexpectedResult)?
                            )
                        )
                    )?;

                    let overlapping_groups = get_overlapping_groups(&transformed_arguments);
                    let transformed_values = overlapping_groups
                        .iter()
                        .map(|group| {
                            // Extract arguments for each group
                            let group_arguments = transformed_arguments
                                .iter()
                                .map(|argument| argument.get(group).cloned())
                                .flatten()
                                .flatten()
                                .collect::<Vec<_>>();

                            // If an argument lacks a group, then the flattening above would make us loose an argument
                            if group_arguments.len() == transformed_arguments.len() {
                                (group.clone(), function.apply(&group_arguments))
                            } else {
                                (group.clone(), None)
                            }
                        })
                        .collect();
                    Ok(OperationResult::GroupValues(sorted_group_values(transformed_values)))
                } else {
                    let transformed_arguments = transform_with_result(
                        transformed_arguments.into_iter(),
                        |argument| argument.value().ok_or_else(|| MetricsEngineError::UnexpectedResult)
                    )?;

                    Ok(OperationResult::Value(function.apply(&transformed_arguments)))
                }
            }
        }
    }

    fn sorted_group_values(mut values: GroupValues) -> GroupValues {
        values.sort_by(|x, y| x.0.cmp(&y.0));
        values
    }

    evaluate(engine, query.time_range, query.expression)
}

pub fn query_in_window<T: MetricQueryable>(engine: &T, query: MetricQuery, duration: Duration) -> MetricsEngineResult<OperationResult> {
    fn evaluate<T: MetricQueryable>(this: &T, time_range: TimeRange, duration: Duration, expression: MetricQueryExpression) -> MetricsEngineResult<OperationResult> {
        match expression {
            MetricQueryExpression::Average { metric, mut query } => {
                query.time_range = time_range;
                query.remove_empty_datapoints = false;
                this.average_in_window(&metric, query, duration)
            }
            MetricQueryExpression::Sum { metric, mut query } => {
                query.time_range = time_range;
                query.remove_empty_datapoints = false;
                this.sum_in_window(&metric, query, duration)
            }
            MetricQueryExpression::Max { metric, mut query } => {
                query.time_range = time_range;
                query.remove_empty_datapoints = false;
                this.max_in_window(&metric, query, duration)
            }
            MetricQueryExpression::Min { metric, mut query } => {
                query.time_range = time_range;
                query.remove_empty_datapoints = false;
                this.min_in_window(&metric, query, duration)
            }
            MetricQueryExpression::Percentile { metric, mut query, percentile } => {
                query.time_range = time_range;
                query.remove_empty_datapoints = false;
                this.percentile_in_window(&metric, query, duration, percentile)
            }
            MetricQueryExpression::Value(value) => {
                Ok(OperationResult::Value(Some(value)))
            }
            MetricQueryExpression::Arithmetic { operation, left, right } => {
                let left = evaluate(this, time_range, duration, *left)?;
                let right = evaluate(this, time_range, duration, *right)?;

                match (left, right) {
                    (OperationResult::TimeValues(left), OperationResult::TimeValues(right)) => {
                        Ok(OperationResult::TimeValues(transform_time_values(&left, &right, |x, y| operation.apply(x, y))))
                    },
                    (OperationResult::TimeValues(left), OperationResult::Value(right)) => {
                        let right = constant_time_values(&left, right);
                        Ok(OperationResult::TimeValues(transform_time_values(&left, &right, |x, y| operation.apply(x, y))))
                    }
                    (OperationResult::Value(left), OperationResult::TimeValues(right)) => {
                        let left = constant_time_values(&right, left);
                        Ok(OperationResult::TimeValues(transform_time_values(&left, &right, |x, y| operation.apply(x, y))))
                    }
                    (OperationResult::Value(left), OperationResult::Value(right)) => {
                        return Ok(OperationResult::Value(option_op(left, right, |x, y| operation.apply(x, y))));
                    }
                    (OperationResult::GroupTimeValues(left), OperationResult::GroupTimeValues(right)) => {
                        let left = group_map(left);
                        let left_groups = group_keys(&left);
                        let right = group_map(right);
                        let right_groups = group_keys(&right);

                        let transformed_values = left_groups.intersection(&right_groups)
                            .map(|&group| {
                                if let (Some(left), Some(right)) = (left.get(group), right.get(group)) {
                                    Some((group.to_owned(), transform_time_values(left, right, |x, y| operation.apply(x, y))))
                                } else {
                                    None
                                }
                            })
                            .flatten()
                            .collect();
                        Ok(OperationResult::GroupTimeValues(sorted_time_group_values(transformed_values)))
                    }
                    (OperationResult::GroupTimeValues(left), OperationResult::Value(right)) => {
                        let transformed_values = left
                            .into_iter()
                            .map(|(group, left)| {
                                let right = constant_time_values(&left, right);
                                (group, transform_time_values(&left, &right, |x, y| operation.apply(x, y)))
                            })
                            .collect();
                        Ok(OperationResult::GroupTimeValues(sorted_time_group_values(transformed_values)))
                    }
                    (OperationResult::Value(left), OperationResult::GroupTimeValues(right)) => {
                        let transformed_values = right
                            .into_iter()
                            .map(|(group, right)| {
                                let left = constant_time_values(&right, left);
                                (group, transform_time_values(&left, &right, |x, y| operation.apply(x, y)))
                            })
                            .collect();
                        Ok(OperationResult::GroupTimeValues(sorted_time_group_values(transformed_values)))
                    }
                    _ => { return Err(MetricsEngineError::UnexpectedResult); }
                }
            }
            MetricQueryExpression::Function { function, arguments } => {
                let num_arguments = arguments.len();
                let transformed_arguments = transform_with_result(
                    arguments.into_iter(),
                    |argument| evaluate(this, time_range, duration, argument)
                )?;

                let num_windows = transformed_arguments
                    .get(0)
                    .map(|arg| arg.num_windows())
                    .flatten()
                    .ok_or_else(|| MetricsEngineError::UnexpectedResult)?;

                if transformed_arguments[0].is_group_time_values() {
                    let transformed_arguments = transform_with_result::<_, _, MetricsEngineError>(
                        transformed_arguments.into_iter(),
                        |argument| Ok(
                            group_map(
                                argument
                                    .group_time_values()
                                    .ok_or_else(|| MetricsEngineError::UnexpectedResult)?
                            )
                        )
                    )?;

                    let overlapping_groups = get_overlapping_groups(&transformed_arguments);

                    let mut results = Vec::new();
                    for group in overlapping_groups {
                        let mut group_results = Vec::new();
                        for window_index in 0..num_windows {
                            let time = transformed_arguments[0][&group][window_index].0;
                            let this_window_transformed_arguments = transformed_arguments
                                .iter()
                                .map(|windows_argument| windows_argument[&group][window_index].1)
                                .flatten()
                                .collect::<Vec<_>>();

                            if this_window_transformed_arguments.len() == num_arguments {
                                group_results.push((time, function.apply(&this_window_transformed_arguments)));
                            }
                        }

                        results.push((group, group_results));
                    }

                    Ok(OperationResult::GroupTimeValues(sorted_time_group_values(results)))
                } else {
                    let transformed_arguments = transform_with_result::<_, _, MetricsEngineError>(
                        transformed_arguments.into_iter(),
                        |argument| argument.time_values().ok_or_else(|| MetricsEngineError::UnexpectedResult)
                    )?;

                    let mut results = Vec::new();
                    for window_index in 0..num_windows {
                        let time = transformed_arguments[0][window_index].0;
                        let this_window_transformed_arguments = transformed_arguments
                            .iter()
                            .map(|windows_argument| windows_argument[window_index].1)
                            .flatten()
                            .collect::<Vec<_>>();

                        if this_window_transformed_arguments.len() == num_arguments {
                            results.push((time, function.apply(&this_window_transformed_arguments)));
                        }
                    }

                    Ok(OperationResult::TimeValues(results))
                }
            }
        }
    }

    fn transform_time_values(left: &TimeValues, right: &TimeValues, op: impl Fn(f64, f64) -> f64) -> TimeValues {
        let mut results = Vec::new();
        for ((left_time, left_value), (right_time, right_value)) in left.iter().zip(right.iter()) {
            assert_eq!(left_time, right_time);

            let result = if let (Some(left), Some(right)) = (left_value, right_value) {
                Some(op(*left, *right))
            } else {
                None
            };

            results.push((*left_time, result));
        }

        results
    }

    fn constant_time_values(time_values: &TimeValues, constant: Option<f64>) -> TimeValues {
        time_values.iter().map(|(time, _)| (*time, constant)).collect()
    }

    fn filter_time_values(time_values: TimeValues) -> TimeValues {
        time_values.into_iter().filter(|(_, value)| value.is_some()).collect()
    }

    fn sorted_time_group_values(mut values: GroupTimeValues) -> GroupTimeValues {
        values.sort_by(|x, y| x.0.cmp(&y.0));
        values
    }

    match evaluate(engine, query.time_range, duration, query.expression)? {
        OperationResult::TimeValues(time_values) => Ok(OperationResult::TimeValues(filter_time_values(time_values))),
        OperationResult::GroupTimeValues(group_time_values) => {
            Ok(
                OperationResult::GroupTimeValues(
                    group_time_values
                        .into_iter()
                        .map(|(group, time_values)| (group, filter_time_values(time_values)))
                        .filter(|(_, values)| !values.is_empty())
                        .collect()
                )
            )
        }
        _ => { Err(MetricsEngineError::UnexpectedResult) }
    }
}

pub trait MetricQueryable {
    fn average(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult>;
    fn sum(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult>;
    fn max(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult>;
    fn min(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult>;
    fn percentile(&self, metric: &str, query: Query, percentile: i32) -> MetricsEngineResult<OperationResult>;

    fn average_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult>;
    fn sum_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult>;
    fn max_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult>;
    fn min_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult>;
    fn percentile_in_window(&self, metric: &str, query: Query, duration: Duration, percentile: i32) -> MetricsEngineResult<OperationResult>;
}

impl MetricQueryable for MetricsEngine {
    fn average(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        self.average(metric, query)
    }

    fn sum(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        self.sum(metric, query)
    }

    fn max(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        self.max(metric, query)
    }

    fn min(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        self.min(metric, query)
    }

    fn percentile(&self, metric: &str, query: Query, percentile: i32) -> MetricsEngineResult<OperationResult> {
        self.percentile(metric, query, percentile)
    }

    fn average_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.average_in_window(metric, query, duration)
    }

    fn sum_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.sum_in_window(metric, query, duration)
    }

    fn max_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.max_in_window(metric, query, duration)
    }

    fn min_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.min_in_window(metric, query, duration)
    }

    fn percentile_in_window(&self, metric: &str, query: Query, duration: Duration, percentile: i32) -> MetricsEngineResult<OperationResult> {
        self.percentile_in_window(metric, query, duration, percentile)
    }
}

fn group_map<T>(values: Vec<(String, T)>) -> FnvHashMap<String, T> {
    FnvHashMap::from_iter(values.into_iter())
}

fn group_keys<T>(map: &FnvHashMap<String, T>) -> FnvHashSet<&String> {
    FnvHashSet::from_iter(map.keys())
}

fn get_overlapping_groups<T>(groups: &[FnvHashMap<String, T>]) -> FnvHashSet<String> {
    let mut overlapping_groups = FnvHashSet::<String>::from_iter(groups[0].keys().cloned());
    for group in groups.iter().skip(1) {
        overlapping_groups = FnvHashSet::from_iter(
            overlapping_groups.intersection(&FnvHashSet::from_iter(group.keys().cloned())).cloned()
        );
    }

    overlapping_groups
}

fn transform_with_result<TIn, TOut, E>(iterator: impl Iterator<Item=TIn>, apply: impl Fn(TIn) -> Result<TOut, E>) -> Result<Vec<TOut>, E> {
    let mut transformed = Vec::new();

    for item in iterator {
        transformed.push(apply(item)?);
    }

    Ok(transformed)
}

fn option_op(left: Option<f64>, right: Option<f64>, op: impl Fn(f64, f64) -> f64) -> Option<f64> {
    if let (Some(left), Some(right)) = (left, right) {
        Some(op(left, right))
    } else {
        None
    }
}

struct TestMetricsEngine {
    metric_values: HashMap<String, OperationResult>
}

impl TestMetricsEngine {
    pub fn new(metric_values: Vec<(String, OperationResult)>) -> TestMetricsEngine {
        TestMetricsEngine {
            metric_values: HashMap::from_iter(metric_values.into_iter())
        }
    }
}

impl MetricQueryable for TestMetricsEngine {
    fn average(&self, metric: &str, _query: Query) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn sum(&self, metric: &str, _query: Query) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn max(&self, metric: &str, _query: Query) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn min(&self, metric: &str, _query: Query) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn percentile(&self, metric: &str, _query: Query, _percentile: i32) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn average_in_window(&self, metric: &str, _query: Query, _duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn sum_in_window(&self, metric: &str, _query: Query, _duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn max_in_window(&self, metric: &str, _query: Query, _duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn min_in_window(&self, metric: &str, _query: Query, _duration: Duration) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }

    fn percentile_in_window(&self, metric: &str, _query: Query, _duration: Duration, _percentile: i32) -> MetricsEngineResult<OperationResult> {
        self.metric_values.get(metric).cloned().ok_or_else(|| MetricsEngineError::UnexpectedResult)
    }
}

#[test]
fn test_query1() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::Value(Some(2.0))),
        ("m2".to_owned(), OperationResult::Value(Some(4.0)))
    ]);

    assert_eq!(
        Some(OperationResult::Value(Some(8.0))),
        query(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Multiply,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() })
                }
            }
        ).ok()
    )
}

#[test]
fn test_query2() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::Value(Some(2.0))),
        ("m2".to_owned(), OperationResult::Value(Some(4.0)))
    ]);

    assert_eq!(
        Some(OperationResult::Value(Some(4.0))),
        query(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() },
                        MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() }
                    ]
                }
            }
        ).ok()
    )
}

#[test]
fn test_query_group1() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::GroupValues(vec![("v1".to_owned(), Some(2.0)), ("v2".to_owned(), Some(3.0))])),
        ("m2".to_owned(), OperationResult::GroupValues(vec![("v2".to_owned(), Some(4.0)), ("v3".to_owned(), Some(5.0))])),
    ]);

    assert_eq!(
        Some(OperationResult::GroupValues(vec![("v2".to_owned(), Some(7.0))])),
        query(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Add,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() })
                }
            }
        ).ok()
    )
}

#[test]
fn test_query_group2() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::GroupValues(vec![("v1".to_owned(), Some(2.0)), ("v2".to_owned(), Some(3.0))]))
    ]);

    assert_eq!(
        Some(OperationResult::GroupValues(vec![("v1".to_owned(), Some(4.0)), ("v2".to_owned(), Some(6.0))])),
        query(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Multiply,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Value(2.0))
                }
            }
        ).ok()
    )
}

#[test]
fn test_query_group3() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::GroupValues(vec![("v1".to_owned(), Some(2.0)), ("v2".to_owned(), Some(3.0))])),
        ("m2".to_owned(), OperationResult::GroupValues(vec![("v2".to_owned(), Some(4.0)), ("v3".to_owned(), Some(5.0))])),
    ]);

    assert_eq!(
        Some(OperationResult::GroupValues(vec![("v2".to_owned(), Some(4.0))])),
        query(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() },
                        MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() }
                    ]
                }
            }
        ).ok()
    )
}

#[test]
fn test_query_in_window1() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::TimeValues(vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)])),
        ("m2".to_owned(), OperationResult::TimeValues(vec![(0.0, None), (1.0, Some(4.0)), (2.0, Some(5.0)), (3.0, Some(6.0))]))
    ]);

    assert_eq!(
        Some(OperationResult::TimeValues(vec![(1.0, Some(6.0)), (2.0, Some(8.0))])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Add,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() })
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}

#[test]
fn test_query_in_window2() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::TimeValues(vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)]))
    ]);

    assert_eq!(
        Some(OperationResult::TimeValues(vec![(0.0, Some(2.0)), (1.0, Some(4.0)), (2.0, Some(6.0))])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Multiply,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Value(2.0))
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}

#[test]
fn test_query_in_window3() {
    let engine = TestMetricsEngine::new(vec![
        ("m1".to_owned(), OperationResult::TimeValues(vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)])),
        ("m2".to_owned(), OperationResult::TimeValues(vec![(0.0, None), (1.0, Some(4.0)), (2.0, Some(5.0)), (3.0, Some(6.0))]))
    ]);

    assert_eq!(
        Some(OperationResult::TimeValues(vec![(1.0, Some(4.0)), (2.0, Some(5.0))])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() },
                        MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() }
                    ]
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}

#[test]
fn test_query_in_window_group1() {
    let engine = TestMetricsEngine::new(vec![
        (
            "m1".to_owned(),
            OperationResult::GroupTimeValues(vec![
                ("t1".to_owned(), vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)]),
                ("t2".to_owned(), vec![(0.0, Some(10.0)), (1.0, Some(20.0)), (2.0, Some(30.0)), (3.0, None)])
            ])
        ),
        (
            "m2".to_owned(),
            OperationResult::GroupTimeValues(vec![
                ("t1".to_owned(), vec![(0.0, None), (1.0, Some(4.0)), (2.0, Some(5.0)), (3.0, Some(6.0))]),
                ("t2".to_owned(), vec![(0.0, None), (1.0, Some(40.0)), (2.0, Some(50.0)), (3.0, Some(60.0))])
            ])
        ),
    ]);

    assert_eq!(
        Some(OperationResult::GroupTimeValues(vec![
            ("t1".to_owned(), vec![(1.0, Some(6.0)), (2.0, Some(8.0))]),
            ("t2".to_owned(), vec![(1.0, Some(60.0)), (2.0, Some(80.0))]),
        ])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Add,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() })
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}

#[test]
fn test_query_in_window_group2() {
    let engine = TestMetricsEngine::new(vec![
        (
            "m1".to_owned(),
            OperationResult::GroupTimeValues(vec![
                ("t1".to_owned(), vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)]),
                ("t2".to_owned(), vec![(0.0, Some(10.0)), (1.0, Some(20.0)), (2.0, Some(30.0)), (3.0, None)])
            ])
        )
    ]);

    assert_eq!(
        Some(OperationResult::GroupTimeValues(vec![
            ("t1".to_owned(), vec![(0.0, Some(10.0)), (1.0, Some(20.0)), (2.0, Some(30.0))]),
            ("t2".to_owned(), vec![(0.0, Some(100.0)), (1.0, Some(200.0)), (2.0, Some(300.0))]),
        ])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Arithmetic {
                    operation: ArithmeticOperation::Multiply,
                    left: Box::new(MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() }),
                    right: Box::new(MetricQueryExpression::Value(10.0))
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}

#[test]
fn test_query_in_window_group3() {
    let engine = TestMetricsEngine::new(vec![
        (
            "m1".to_owned(),
            OperationResult::GroupTimeValues(vec![
                ("t1".to_owned(), vec![(0.0, Some(1.0)), (1.0, Some(2.0)), (2.0, Some(3.0)), (3.0, None)]),
                ("t2".to_owned(), vec![(0.0, Some(10.0)), (1.0, Some(20.0)), (2.0, Some(30.0)), (3.0, None)])
            ])
        ),
        (
            "m2".to_owned(),
            OperationResult::GroupTimeValues(vec![
                ("t1".to_owned(), vec![(0.0, None), (1.0, Some(4.0)), (2.0, Some(5.0)), (3.0, Some(6.0))]),
                ("t2".to_owned(), vec![(0.0, None), (1.0, Some(40.0)), (2.0, Some(50.0)), (3.0, Some(60.0))])
            ])
        ),
    ]);

    assert_eq!(
        Some(OperationResult::GroupTimeValues(vec![
            ("t1".to_owned(), vec![(1.0, Some(4.0)), (2.0, Some(5.0))]),
            ("t2".to_owned(), vec![(1.0, Some(40.0)), (2.0, Some(50.0))]),
        ])),
        query_in_window(
            &engine,
            MetricQuery {
                time_range: TimeRange::new(0.0, 1.0),
                expression: MetricQueryExpression::Function {
                    function: Function::Max,
                    arguments: vec![
                        MetricQueryExpression::Average { metric: "m1".to_string(), query: Query::placeholder() },
                        MetricQueryExpression::Average { metric: "m2".to_string(), query: Query::placeholder() }
                    ]
                }
            },
            Duration::from_secs_f64(1.0)
        ).ok()
    )
}