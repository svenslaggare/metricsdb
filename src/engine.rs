use std::path::{Path, PathBuf};
use fnv::FnvHashMap;

use crate::{DefaultCountMetric, DefaultGaugeMetric, Query};
use crate::model::{MetricError};

pub type MetricsEngineResult<T> = Result<T, MetricsEngineError>;

#[derive(Debug)]
pub enum MetricsEngineError {
    FailedToCreateBaseDir(std::io::Error),
    MetricAlreadyExists,
    MetricNotFound,
    WrongMetricType,
    FailedToSaveMetricDefinitions(std::io::Error),
    Metric(MetricError)
}

impl From<MetricError> for MetricsEngineError {
    fn from(other: MetricError) -> Self {
        MetricsEngineError::Metric(other)
    }
}

pub struct MetricsEngine {
    base_path: PathBuf,
    metrics: FnvHashMap<String, Metric>
}

impl MetricsEngine {
    pub fn new(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricError::FailedToCreateBaseDir(err))?;
        }

        Ok(
            MetricsEngine {
                base_path: base_path.to_owned(),
                metrics: FnvHashMap::default()
            }
        )
    }

    pub fn add_gauge_metric(&mut self, name: &str) -> MetricsEngineResult<()> {
        if self.metrics.contains_key(name) {
            return Err(MetricsEngineError::MetricAlreadyExists);
        }

        self.metrics.insert(name.to_string(), Metric::Gauge(DefaultGaugeMetric::new(&self.base_path.join(name))?));
        self.save_defined_metrics()?;
        Ok(())
    }

    pub fn add_count_metric(&mut self, name: &str) -> MetricsEngineResult<()> {
        if self.metrics.contains_key(name) {
            return Err(MetricsEngineError::MetricAlreadyExists);
        }

        self.metrics.insert(name.to_string(), Metric::Count(DefaultCountMetric::new(&self.base_path.join(name))?));
        self.save_defined_metrics()?;
        Ok(())
    }

    fn save_defined_metrics(&self) -> MetricsEngineResult<()> {
        let save = || -> std::io::Result<()> {
            let content = serde_json::to_string(&self.metrics.keys().collect::<Vec<_>>())?;
            std::fs::write(&self.base_path.join("metrics.json"), &content)?;
            Ok(())
        };

        save().map_err(|err| MetricsEngineError::FailedToSaveMetricDefinitions(err))?;
        Ok(())
    }

    pub fn gauge(&mut self, name: &str, time: f64, value: f64, tags: &[&str]) -> MetricsEngineResult<()> {
        if let Metric::Gauge(metric) = self.metrics.get_mut(name).ok_or_else(|| MetricsEngineError::MetricNotFound)? {
            metric.add(time, value, tags)?;
            Ok(())
        } else {
            return Err(MetricsEngineError::WrongMetricType);
        }
    }

    pub fn count(&mut self, name: &str, time: f64, count: u16, tags: &[&str]) -> MetricsEngineResult<()> {
        if let Metric::Count(metric) = self.metrics.get_mut(name).ok_or_else(|| MetricsEngineError::MetricNotFound)? {
            metric.add(time, count, tags)?;
            Ok(())
        } else {
            return Err(MetricsEngineError::WrongMetricType);
        }
    }

    pub fn average(&self, name: &str, query: Query) -> Option<f64> {
        match self.metrics.get(name)? {
            Metric::Gauge(metric) => metric.average(query),
            Metric::Count(metric) => metric.average(query)
        }
    }

    pub fn sum(&self, name: &str, query: Query) -> Option<f64> {
        match self.metrics.get(name)? {
            Metric::Gauge(metric) => metric.sum(query),
            Metric::Count(metric) => metric.sum(query)
        }
    }
}

pub enum Metric {
    Gauge(DefaultGaugeMetric),
    Count(DefaultCountMetric)
}