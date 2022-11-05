pub mod common;
pub mod tags;
pub mod gauge;
pub mod count;
mod metric_operations;
pub mod operations;

use std::fmt::{Display};
use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
pub enum OperationResult {
    Value(Option<f64>),
    TimeValues(Vec<(f64, f64)>),
    GroupValues(Vec<(String, Option<f64>)>),
    GroupTimeValues(Vec<(String, Vec<(f64, f64)>)>)
}

impl OperationResult {
    pub fn value(self) -> Option<f64> {
        match self {
            OperationResult::Value(value) => value,
            _ => None
        }
    }

    pub fn time_values(self) -> Option<Vec<(f64, f64)>> {
        match self {
            OperationResult::TimeValues(values) => Some(values),
            _ => None
        }
    }

    pub fn as_json(&self) -> serde_json::Value {
        match self {
            OperationResult::Value(value) => json!(value),
            OperationResult::TimeValues(values) => json!(values),
            OperationResult::GroupValues(values) => json!(values),
            OperationResult::GroupTimeValues(values) => json!(values)
        }
    }
}

impl Display for OperationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationResult::Value(Some(value)) => write!(f, "{}", value),
            OperationResult::Value(None) => write!(f, "None"),
            OperationResult::TimeValues(values) => write!(f, "{:?}", values),
            OperationResult::GroupValues(values) => write!(f, "{:?}", values),
            OperationResult::GroupTimeValues(values) => write!(f, "{:?}", values)
        }
    }
}