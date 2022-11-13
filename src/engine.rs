use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use fnv::{FnvBuildHasher};

use serde::{Serialize, Deserialize};
use crate::metric::common::{CountInput, GenericMetric};

use crate::metric::count::DefaultCountMetric;
use crate::metric::expression::{ArithmeticOperation, Function};
use crate::metric::gauge::DefaultGaugeMetric;
use crate::metric::{OperationResult, TimeValues};
use crate::metric::ratio::{DefaultRatioMetric, RatioInput};
use crate::metric::tags::{PrimaryTag, Tag};
use crate::model::{MetricError, Query, TimeRange};

pub type MetricsEngineResult<T> = Result<T, MetricsEngineError>;

#[derive(Debug)]
pub enum MetricsEngineError {
    FailedToCreateBaseDir(std::io::Error),
    FailedToLoadMetricDefinitions(std::io::Error),
    FailedToSaveMetricDefinitions(std::io::Error),
    MetricAlreadyExists,
    MetricNotFound,
    WrongMetricType,
    UnexpectedResult,
    InvalidQueryInput,
    Metric(MetricError)
}

impl From<MetricError> for MetricsEngineError {
    fn from(other: MetricError) -> Self {
        MetricsEngineError::Metric(other)
    }
}

#[derive(Serialize, Deserialize)]
pub struct AddGaugeValue {
    pub time: f64,
    pub value: f64,
    pub tags: Vec<Tag>
}

impl AddGaugeValue {
    pub fn new(time: f64, value: f64, tags: Vec<Tag>) -> AddGaugeValue {
        AddGaugeValue {
            time,
            value,
            tags
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct AddCountValue {
    pub time: f64,
    pub count: CountInput,
    pub tags: Vec<Tag>
}

impl AddCountValue {
    pub fn new(time: f64, count: CountInput, tags: Vec<Tag>) -> AddCountValue {
        AddCountValue {
            time,
            count,
            tags
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct AddRatioValue {
    pub time: f64,
    pub numerator: u32,
    pub denominator: u32,
    pub tags: Vec<Tag>
}

impl AddRatioValue {
    pub fn new(time: f64, numerator: u32, denominator: u32, tags: Vec<Tag>) -> AddRatioValue {
        AddRatioValue {
            time,
            numerator,
            denominator,
            tags
        }
    }
}

pub struct MetricsEngine {
    base_path: PathBuf,
    metrics: DashMap<String, ArcMetric, FnvBuildHasher>,
    create_lock: Mutex<()>
}

impl MetricsEngine {
    pub fn new(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricsEngineError::FailedToCreateBaseDir(err))?;
        }

        Ok(
            MetricsEngine {
                base_path: base_path.to_owned(),
                metrics: DashMap::default(),
                create_lock: Mutex::new(())
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        let load = || -> std::io::Result<Vec<(String, MetricType)>> {
            let content = std::fs::read_to_string(&base_path.join("metrics.json"))?;
            let metrics: Vec<_> = serde_json::from_str(&content)?;
            Ok(metrics)
        };

        let metrics = DashMap::default();
        for (metric_name, metric_type) in load().map_err(|err| MetricsEngineError::FailedToLoadMetricDefinitions(err))? {
            let metric = match metric_type {
                MetricType::Gauge => Metric::Gauge(DefaultGaugeMetric::from_existing(&base_path.join(&metric_name))?),
                MetricType::Count => Metric::Count(DefaultCountMetric::from_existing(&base_path.join(&metric_name))?),
                MetricType::Ratio => Metric::Ratio(DefaultRatioMetric::from_existing(&base_path.join(&metric_name))?)
            };

            metrics.insert(metric_name, Arc::new(RwLock::new(metric)));
        }

        Ok(
            MetricsEngine {
                base_path: base_path.to_owned(),
                metrics,
                create_lock: Mutex::new(())
            }
        )
    }

    pub fn new_or_from_existing(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        if base_path.join("metrics.json").exists() {
            MetricsEngine::from_existing(base_path)
        } else {
            MetricsEngine::new(base_path)
        }
    }

    pub fn add_gauge_metric(&self, name: &str) -> MetricsEngineResult<()> {
        let _guard = self.create_lock.lock().unwrap();
        if self.metrics.contains_key(name) {
            return Err(MetricsEngineError::MetricAlreadyExists);
        }

        self.metrics.insert(
            name.to_string(),
            Metric::gauge(DefaultGaugeMetric::new(&self.base_path.join(name))?)
        );

        self.save_defined_metrics()?;
        Ok(())
    }

    pub fn add_count_metric(&self, name: &str) -> MetricsEngineResult<()> {
        let _guard = self.create_lock.lock().unwrap();
        if self.metrics.contains_key(name) {
            return Err(MetricsEngineError::MetricAlreadyExists);
        }

        self.metrics.insert(
            name.to_string(),
            Metric::count(DefaultCountMetric::new(&self.base_path.join(name))?)
        );

        self.save_defined_metrics()?;
        Ok(())
    }

    pub fn add_ratio_metric(&self, name: &str) -> MetricsEngineResult<()> {
        let _guard = self.create_lock.lock().unwrap();
        if self.metrics.contains_key(name) {
            return Err(MetricsEngineError::MetricAlreadyExists);
        }

        self.metrics.insert(
            name.to_string(),
            Metric::ratio(DefaultRatioMetric::new(&self.base_path.join(name))?)
        );

        self.save_defined_metrics()?;
        Ok(())
    }

    fn save_defined_metrics(&self) -> MetricsEngineResult<()> {
        let save = || -> std::io::Result<()> {
            let content = serde_json::to_string(
                &self.metrics
                    .iter()
                    .map(|item| (item.key().to_owned(), item.value().read().unwrap().metric_type()))
                    .collect::<Vec<_>>()
            )?;
            std::fs::write(&self.base_path.join("metrics.json"), &content)?;
            Ok(())
        };

        save().map_err(|err| MetricsEngineError::FailedToSaveMetricDefinitions(err))?;
        Ok(())
    }

    pub fn add_auto_primary_tag(&self, metric: &str, key: &str) -> MetricsEngineResult<()> {
        match self.metrics.get_metric(metric)?.write().unwrap().deref_mut() {
            Metric::Gauge(metric) => metric.add_auto_primary_tag(key)?,
            Metric::Count(metric) => metric.add_auto_primary_tag(key)?,
            Metric::Ratio(metric) => metric.add_auto_primary_tag(key)?,
        }

        Ok(())
    }

    pub fn add_primary_tag(&self, metric: &str, tag: PrimaryTag) -> MetricsEngineResult<()> {
        match self.metrics.get_metric(metric)?.write().unwrap().deref_mut() {
            Metric::Gauge(metric) => metric.add_primary_tag(tag)?,
            Metric::Count(metric) => metric.add_primary_tag(tag)?,
            Metric::Ratio(metric) => metric.add_primary_tag(tag)?,
        }

        Ok(())
    }

    pub fn gauge(&self, metric: &str, values: impl Iterator<Item=AddGaugeValue>) -> MetricsEngineResult<usize> {
        match self.metrics.get_metric(metric)?.write().unwrap().deref_mut() {
            Metric::Gauge(metric) => {
                let mut num_success = 0;
                let mut error = None;

                for value in values {
                    match metric.add(value.time, value.value, value.tags) {
                        Ok(_) => { num_success += 1; }
                        Err(err) => { error = Some(err); }
                    }
                }

                if num_success == 0 {
                    if let Some(err) = error {
                        return Err(err.into());
                    }
                }

                Ok(num_success)
            }
            _ => Err(MetricsEngineError::WrongMetricType)
        }
    }

    pub fn count(&self, metric: &str, values: impl Iterator<Item=AddCountValue>) -> MetricsEngineResult<usize> {
        match self.metrics.get_metric(metric)?.write().unwrap().deref_mut() {
            Metric::Count(metric) => {
                let mut num_success = 0;
                let mut error = None;

                for value in values {
                    match metric.add(value.time, value.count, value.tags) {
                        Ok(_) => { num_success += 1; }
                        Err(err) => { error = Some(err); }
                    }
                }

                if num_success == 0 {
                    if let Some(err) = error {
                        return Err(err.into());
                    }
                }

                Ok(num_success)
            }
            _ => Err(MetricsEngineError::WrongMetricType)
        }
    }

    pub fn ratio(&self, metric: &str, values: impl Iterator<Item=AddRatioValue>) -> MetricsEngineResult<usize> {
        match self.metrics.get_metric(metric)?.write().unwrap().deref_mut() {
            Metric::Ratio(metric) => {
                let mut num_success = 0;
                let mut error = None;

                for value in values {
                    match metric.add(value.time, RatioInput(CountInput(value.numerator), CountInput(value.denominator)), value.tags) {
                        Ok(_) => { num_success += 1; }
                        Err(err) => { error = Some(err); }
                    }
                }

                if num_success == 0 {
                    if let Some(err) = error {
                        return Err(err.into());
                    }
                }

                Ok(num_success)
            }
            _ => Err(MetricsEngineError::WrongMetricType)
        }
    }

    pub fn average(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.average(query)),
            Metric::Count(metric) => Ok(metric.average(query)),
            Metric::Ratio(metric) => Ok(metric.average(query))
        }
    }

    pub fn sum(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.sum(query)),
            Metric::Count(metric) => Ok(metric.sum(query)),
            Metric::Ratio(metric) => Ok(metric.sum(query))
        }
    }

    pub fn max(&self, metric: &str, query: Query) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.max(query)),
            Metric::Count(metric) => Ok(metric.max(query)),
            Metric::Ratio(metric) => Ok(metric.max(query)),
        }
    }

    pub fn percentile(&self, metric: &str, query: Query, percentile: i32) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.percentile(query, percentile)),
            Metric::Count(metric) => Ok(metric.percentile(query, percentile)),
            Metric::Ratio(metric) => Ok(metric.percentile(query, percentile)),
        }
    }

    pub fn query(&self, query: MetricQuery) -> MetricsEngineResult<OperationResult> {
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

        evaluate(self, query.time_range, query.expression)
    }

    pub fn average_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.average_in_window(query, duration)),
            Metric::Count(metric) => Ok(metric.average_in_window(query, duration)),
            Metric::Ratio(metric) => Ok(metric.average_in_window(query, duration))
        }
    }

    pub fn sum_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.sum_in_window(query, duration)),
            Metric::Count(metric) => Ok(metric.sum_in_window(query, duration)),
            Metric::Ratio(metric) => Ok(metric.sum_in_window(query, duration))
        }
    }

    pub fn max_in_window(&self, metric: &str, query: Query, duration: Duration) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.max_in_window(query, duration)),
            Metric::Count(metric) => Ok(metric.max_in_window(query, duration)),
            Metric::Ratio(metric) => Ok(metric.max_in_window(query, duration))
        }
    }

    pub fn percentile_in_window(&self, metric: &str, query: Query, duration: Duration, percentile: i32) -> MetricsEngineResult<OperationResult> {
        match self.metrics.get_metric(metric)?.read().unwrap().deref() {
            Metric::Gauge(metric) => Ok(metric.percentile_in_window(query, duration, percentile)),
            Metric::Count(metric) => Ok(metric.percentile_in_window(query, duration, percentile)),
            Metric::Ratio(metric) => Ok(metric.percentile_in_window(query, duration, percentile))
        }
    }

    pub fn query_in_window(&self, query: MetricQuery, duration: Duration) -> MetricsEngineResult<OperationResult> {
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

        if let Some(time_values) = evaluate(self, query.time_range, duration, query.expression)?.time_values() {
            Ok(OperationResult::TimeValues(time_values.into_iter().filter(|(_, value)| value.is_some()).collect()))
        } else {
            Err(MetricsEngineError::UnexpectedResult)
        }
    }

    pub fn scheduled(&self) {
        for entry in self.metrics.iter() {
            match entry.value().write().unwrap().deref_mut() {
                Metric::Gauge(metric) => metric.scheduled(),
                Metric::Count(metric) => metric.scheduled(),
                Metric::Ratio(metric) => metric.scheduled()
            }
        }
    }
}

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

pub enum MetricQueryExpression {
    Average { metric: String, query: Query },
    Sum { metric: String, query: Query },
    Max { metric: String, query: Query },
    Percentile { metric: String,  query: Query, percentile: i32 },
    Value(f64),
    Arithmetic { operation: ArithmeticOperation, left: Box<MetricQueryExpression>, right: Box<MetricQueryExpression> },
    Function { function: Function, arguments: Vec<MetricQueryExpression> }
}

trait MetricsHashMapExt {
    fn get_metric(&self, name: &str) -> MetricsEngineResult<ArcMetric>;
}

impl MetricsHashMapExt for DashMap<String, ArcMetric, FnvBuildHasher> {
    fn get_metric(&self, name: &str) -> MetricsEngineResult<ArcMetric> {
        self.get(name).ok_or_else(|| MetricsEngineError::MetricNotFound).map(|item| item.value().clone())
    }
}

pub type ArcMetric = Arc<RwLock<Metric>>;

pub enum Metric {
    Gauge(DefaultGaugeMetric),
    Count(DefaultCountMetric),
    Ratio(DefaultRatioMetric)
}

impl Metric {
    pub fn gauge(metric: DefaultGaugeMetric) -> ArcMetric {
        Arc::new(RwLock::new(Metric::Gauge(metric)))
    }

    pub fn count(metric: DefaultCountMetric) -> ArcMetric {
        Arc::new(RwLock::new(Metric::Count(metric)))
    }

    pub fn ratio(metric: DefaultRatioMetric) -> ArcMetric {
        Arc::new(RwLock::new(Metric::Ratio(metric)))
    }

    pub fn metric_type(&self) -> MetricType {
        match self {
            Metric::Gauge(_) => MetricType::Gauge,
            Metric::Count(_) => MetricType::Count,
            Metric::Ratio(_) => MetricType::Ratio
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum MetricType {
    Gauge,
    Count,
    Ratio
}

fn option_op(left: Option<f64>, right: Option<f64>, op: impl Fn(f64, f64) -> f64) -> Option<f64> {
    if let (Some(left), Some(right)) = (left, right) {
        Some(op(left, right))
    } else {
        None
    }
}