use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use fnv::FnvBuildHasher;

use serde::{Deserialize, Serialize};

use crate::engine::io::{AddCountValue, AddGaugeValue, AddRatioValue, MetricsEngineError, MetricsEngineResult};

use crate::engine::querying;
use crate::engine::querying::MetricQuery;
use crate::metric::common::{CountInput, GenericMetric};

use crate::metric::count::DefaultCountMetric;
use crate::metric::gauge::DefaultGaugeMetric;
use crate::metric::OperationResult;
use crate::metric::ratio::{DefaultRatioMetric, RatioInput};
use crate::metric::tags::{PrimaryTag};
use crate::model::Query;

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
        querying::query(self, query)
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
        querying::query_in_window(self, query, duration)
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