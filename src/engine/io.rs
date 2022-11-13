use serde::{Deserialize, Serialize};

use crate::metric::common::CountInput;
use crate::metric::tags::Tag;
use crate::model::MetricError;

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

pub type MetricsEngineResult<T> = Result<T, MetricsEngineError>;

#[derive(Serialize, Deserialize)]
pub struct AddRatioValue {
    pub time: f64,
    pub numerator: u32,
    pub denominator: u32,
    pub tags: Vec<Tag>
}
