use std::path::{Path, PathBuf};
use std::sync::RwLock;

use fnv::FnvHashMap;
use serde::{Serialize, Deserialize};

use crate::{DefaultCountMetric, DefaultGaugeMetric, PrimaryTag, Query};
use crate::model::{MetricError};

pub type MetricsEngineResult<T> = Result<T, MetricsEngineError>;

#[derive(Debug)]
pub enum MetricsEngineError {
    FailedToCreateBaseDir(std::io::Error),
    FailedToLoadMetricDefinitions(std::io::Error),
    FailedToSaveMetricDefinitions(std::io::Error),
    MetricAlreadyExists,
    MetricNotFound,
    WrongMetricType,
    Metric(MetricError)
}

impl From<MetricError> for MetricsEngineError {
    fn from(other: MetricError) -> Self {
        MetricsEngineError::Metric(other)
    }
}

pub struct MetricsEngine {
    base_path: PathBuf,
    metrics: RwLock<FnvHashMap<String, Metric>>
}

impl MetricsEngine {
    pub fn new(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricsEngineError::FailedToCreateBaseDir(err))?;
        }

        Ok(
            MetricsEngine {
                base_path: base_path.to_owned(),
                metrics: RwLock::new(FnvHashMap::default())
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricsEngineResult<MetricsEngine> {
        let load = || -> std::io::Result<Vec<(String, MetricType)>> {
            let content = std::fs::read_to_string(&base_path.join("metrics.json"))?;
            let metrics: Vec<_> = serde_json::from_str(&content)?;
            Ok(metrics)
        };

        let mut metrics = FnvHashMap::default();
        for (metric_name, metric_type) in load().map_err(|err| MetricsEngineError::FailedToLoadMetricDefinitions(err))? {
            let metric = match metric_type {
                MetricType::Gauge => Metric::Gauge(DefaultGaugeMetric::from_existing(&base_path.join(&metric_name))?),
                MetricType::Count => Metric::Count(DefaultCountMetric::from_existing(&base_path.join(&metric_name))?),
            };

            metrics.insert(metric_name, metric);
        }

        Ok(
            MetricsEngine {
                base_path: base_path.to_owned(),
                metrics: RwLock::new(metrics)
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
        {
            let mut metrics_guard = self.metrics.write().unwrap();
            if metrics_guard.contains_key(name) {
                return Err(MetricsEngineError::MetricAlreadyExists);
            }

            metrics_guard.insert(name.to_string(), Metric::Gauge(DefaultGaugeMetric::new(&self.base_path.join(name))?));
        }

        self.save_defined_metrics()?;
        Ok(())
    }

    pub fn add_count_metric(&self, name: &str) -> MetricsEngineResult<()> {
        {
            let mut metrics_guard = self.metrics.write().unwrap();
            if metrics_guard.contains_key(name) {
                return Err(MetricsEngineError::MetricAlreadyExists);
            }

            metrics_guard.insert(name.to_string(), Metric::Count(DefaultCountMetric::new(&self.base_path.join(name))?));
        }

        self.save_defined_metrics()?;
        Ok(())
    }

    fn save_defined_metrics(&self) -> MetricsEngineResult<()> {
        let save = || -> std::io::Result<()> {
            let content = serde_json::to_string(&
                self.metrics.read().unwrap()
                    .iter()
                    .map(|(name, metric)| (name, metric.metric_type()))
                    .collect::<Vec<_>>()
            )?;
            std::fs::write(&self.base_path.join("metrics.json"), &content)?;
            Ok(())
        };

        save().map_err(|err| MetricsEngineError::FailedToSaveMetricDefinitions(err))?;
        Ok(())
    }

    pub fn add_primary_tag(&self, metric: &str, tag: PrimaryTag) -> MetricsEngineResult<()> {
        match self.metrics.write().unwrap().get_mut(metric).ok_or_else(|| MetricsEngineError::MetricNotFound)? {
            Metric::Gauge(metric) => metric.add_primary_tag(tag)?,
            Metric::Count(metric) => metric.add_primary_tag(tag)?,
        }

        Ok(())
    }

    pub fn gauge(&self, metric: &str, time: f64, value: f64, tags: Vec<String>) -> MetricsEngineResult<()> {
        if let Metric::Gauge(metric) = self.metrics.write().unwrap().get_mut(metric).ok_or_else(|| MetricsEngineError::MetricNotFound)? {
            metric.add(time, value, tags)?;
            Ok(())
        } else {
            return Err(MetricsEngineError::WrongMetricType);
        }
    }

    pub fn count(&self, metric: &str, time: f64, count: u16, tags: Vec<String>) -> MetricsEngineResult<()> {
        if let Metric::Count(metric) = self.metrics.write().unwrap().get_mut(metric).ok_or_else(|| MetricsEngineError::MetricNotFound)? {
            metric.add(time, count, tags)?;
            Ok(())
        } else {
            return Err(MetricsEngineError::WrongMetricType);
        }
    }

    pub fn average(&self, metric: &str, query: Query) -> Option<f64> {
        match self.metrics.read().unwrap().get(metric)? {
            Metric::Gauge(metric) => metric.average(query),
            Metric::Count(metric) => metric.average(query)
        }
    }

    pub fn sum(&self, metric: &str, query: Query) -> Option<f64> {
        match self.metrics.read().unwrap().get(metric)? {
            Metric::Gauge(metric) => metric.sum(query),
            Metric::Count(metric) => metric.sum(query)
        }
    }
}

pub enum Metric {
    Gauge(DefaultGaugeMetric),
    Count(DefaultCountMetric)
}

impl Metric {
    pub fn metric_type(&self) -> MetricType {
        match self {
            Metric::Gauge(_) => MetricType::Gauge,
            Metric::Count(_) => MetricType::Count
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum MetricType {
    Gauge,
    Count
}