pub mod gauge;
pub mod count;
pub mod ratio;

pub mod common;
pub mod tags;
mod metric_operations;
pub mod operations;
pub mod expression;

use std::fmt::{Display};
use serde_json::json;

pub type TimeValues = Vec<(f64, Option<f64>)>;
pub type GroupValues = Vec<(String, Option<f64>)>;

#[derive(Debug, Clone, PartialEq)]
pub enum OperationResult {
    NotSupported,
    Value(Option<f64>),
    TimeValues(TimeValues),
    GroupValues(GroupValues),
    GroupTimeValues(Vec<(String, Vec<(f64, Option<f64>)>)>)
}

impl OperationResult {
    pub fn value(self) -> Option<f64> {
        match self {
            OperationResult::Value(value) => value,
            _ => None
        }
    }

    pub fn time_values(self) -> Option<TimeValues> {
        match self {
            OperationResult::TimeValues(values) => Some(values),
            _ => None
        }
    }

    pub fn group_values(self) -> Option<GroupValues> {
        match self {
            OperationResult::GroupValues(values) => Some(values),
            _ => None
        }
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            OperationResult::NotSupported => Some("Not supported operation.".to_owned()),
            _ => None
        }
    }

    pub fn is_group_values(&self) -> bool {
        match self {
            OperationResult::GroupValues(_) => true,
            _ => false
        }
    }

    pub fn as_json(&self) -> serde_json::Value {
        match self {
            OperationResult::NotSupported => json!({ "error_message": "not supported operation" }),
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
            OperationResult::NotSupported => write!(f, "NotSupported"),
            OperationResult::Value(Some(value)) => write!(f, "{}", value),
            OperationResult::Value(None) => write!(f, "None"),
            OperationResult::TimeValues(values) => write!(f, "{:?}", values),
            OperationResult::GroupValues(values) => write!(f, "{:?}", values),
            OperationResult::GroupTimeValues(values) => write!(f, "{:?}", values)
        }
    }
}