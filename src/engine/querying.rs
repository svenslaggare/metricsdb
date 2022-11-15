use std::time::Duration;

use serde::Deserialize;

use crate::engine::engine::MetricsEngine;
use crate::engine::io::{MetricsEngineError, MetricsEngineResult};
use crate::metric::{OperationResult, TimeValues};
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

pub fn query(this: &MetricsEngine, query: MetricQuery) -> MetricsEngineResult<OperationResult> {
    fn evaluate(this: &MetricsEngine, time_range: TimeRange, expression: MetricQueryExpression) -> MetricsEngineResult<OperationResult> {
        match expression {
            MetricQueryExpression::Average { metric, mut query } => {
                query.time_range = time_range;
                this.average(&metric, query)
            }
            MetricQueryExpression::Sum { metric, mut query } => {
                query.time_range = time_range;
                this.sum(&metric, query)
            }
            MetricQueryExpression::Max { metric, mut query } => {
                query.time_range = time_range;
                this.max(&metric, query)
            }
            MetricQueryExpression::Min { metric, mut query } => {
                query.time_range = time_range;
                this.min(&metric, query)
            }
            MetricQueryExpression::Percentile { metric, mut query, percentile } => {
                query.time_range = time_range;
                this.percentile(&metric, query, percentile)
            }
            MetricQueryExpression::Value(value) => {
                Ok(OperationResult::Value(Some(value)))
            }
            MetricQueryExpression::Arithmetic { operation, left, right } => {
                let left = evaluate(this, time_range, *left)?;
                let right = evaluate(this, time_range, *right)?;
                Ok(OperationResult::Value(option_op(left.value(), right.value(), |x, y| operation.apply(x, y))))
            }
            MetricQueryExpression::Function { function, arguments } => {
                let mut transformed_arguments = Vec::new();
                for argument in arguments {
                    transformed_arguments.push(
                        evaluate(this, time_range, argument)?
                            .value()
                            .ok_or_else(|| MetricsEngineError::UnexpectedResult)?
                    );
                }

                Ok(OperationResult::Value(function.apply(&transformed_arguments)))
            }
        }
    }

    evaluate(this, query.time_range, query.expression)
}

pub fn query_in_window(this: &MetricsEngine, query: MetricQuery, duration: Duration) -> MetricsEngineResult<OperationResult> {
    fn evaluate(this: &MetricsEngine, time_range: TimeRange, duration: Duration, expression: MetricQueryExpression) -> MetricsEngineResult<OperationResult> {
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

                let left_constant = left.clone().value();
                let right_constant = right.clone().value();
                let (left, right) = match (left.time_values(), right.time_values()) {
                    (Some(left), Some(right)) => (left, right),
                    (Some(left), None) => {
                        let right = constant_time_values(&left, right_constant);
                        (left, right)
                    },
                    (None, Some(right)) => {
                        let left = constant_time_values(&right, left_constant);
                        (left, right)
                    },
                    (None, None) => {
                        return Ok(OperationResult::Value(option_op(left_constant, right_constant, |x, y| operation.apply(x, y))));
                    }
                };

                Ok(OperationResult::TimeValues(transform_time_values(left, right, |x, y| operation.apply(x, y))))
            }
            MetricQueryExpression::Function { function, arguments } => {
                let mut transformed_arguments = Vec::new();
                let num_arguments = arguments.len();
                for argument in arguments {
                    transformed_arguments.push(
                        evaluate(this, time_range, duration, argument)?
                            .time_values()
                            .ok_or_else(|| MetricsEngineError::UnexpectedResult)?
                    );
                }

                let num_windows = transformed_arguments
                    .get(0)
                    .map(|arg| arg.len())
                    .ok_or_else(|| MetricsEngineError::UnexpectedResult)?;

                let mut results = Vec::new();
                for window_index in 0..num_windows {
                    let time = transformed_arguments[0][window_index].0;
                    let mut this_window_transformed_arguments = Vec::new();
                    for windows_argument in &transformed_arguments {
                        if let Some(value) = windows_argument[window_index].1 {
                            this_window_transformed_arguments.push(value);
                        }
                    }

                    if this_window_transformed_arguments.len() == num_arguments {
                        results.push((time, function.apply(&this_window_transformed_arguments)));
                    }
                }

                Ok(OperationResult::TimeValues(results))
            }
        }
    }

    fn transform_time_values(left: TimeValues, right: TimeValues, op: impl Fn(f64, f64) -> f64) -> TimeValues {
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

    if let Some(time_values) = evaluate(this, query.time_range, duration, query.expression)?.time_values() {
        Ok(OperationResult::TimeValues(time_values.into_iter().filter(|(_, value)| value.is_some()).collect()))
    } else {
        Err(MetricsEngineError::UnexpectedResult)
    }
}

fn option_op(left: Option<f64>, right: Option<f64>, op: impl Fn(f64, f64) -> f64) -> Option<f64> {
    if let (Some(left), Some(right)) = (left, right) {
        Some(op(left, right))
    } else {
        None
    }
}