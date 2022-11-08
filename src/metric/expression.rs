use serde::{Deserialize, Serialize};
use crate::metric::ratio::Ratio;

pub enum ExpressionValue {
    Float(f64),
    Ratio(Ratio)
}

impl ExpressionValue {
    pub fn float(&self) -> Option<f64> {
        match self {
            ExpressionValue::Float(value) => Some(*value),
            ExpressionValue::Ratio(value) => value.value()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TransformExpression {
    InputValue,
    Value { value: f64 },
    Arithmetic { operation: ArithmeticOperation, left: Box<TransformExpression>, right: Box<TransformExpression> },
    Function { function: Function, arguments: Vec<TransformExpression> }
}

impl TransformExpression {
    pub fn evaluate(&self, input: &ExpressionValue) -> Option<f64> {
        match self {
            TransformExpression::InputValue => input.float(),
            TransformExpression::Value { value } => Some(*value),
            TransformExpression::Arithmetic { operation, left, right } => {
                let left = left.evaluate(input)?;
                let right = right.evaluate(input)?;

                match operation {
                    ArithmeticOperation::Add => Some(left + right),
                    ArithmeticOperation::Subtract => Some(left - right),
                    ArithmeticOperation::Multiply => Some(left * right),
                    ArithmeticOperation::Divide => Some(left / right),
                }
            }
            TransformExpression::Function { function, arguments } => {
                let mut transformed_arguments = Vec::new();
                for argument in arguments {
                    transformed_arguments.push(argument.evaluate(input)?);
                }

                match function {
                    Function::Abs if transformed_arguments.len() == 1 => Some(transformed_arguments[0].abs()),
                    Function::Max if transformed_arguments.len() == 2 => Some(transformed_arguments[0].max(transformed_arguments[1])),
                    Function::Min if transformed_arguments.len() == 2 => Some(transformed_arguments[0].min(transformed_arguments[1])),
                    Function::Round if transformed_arguments.len() == 1 => Some(transformed_arguments[0].round()),
                    Function::Ceil if transformed_arguments.len() == 1 => Some(transformed_arguments[0].ceil()),
                    Function::Floor if transformed_arguments.len() == 1 => Some(transformed_arguments[0].floor()),
                    Function::Sqrt if transformed_arguments.len() == 1 && transformed_arguments[0] >= 0.0 => Some(transformed_arguments[0].sqrt()),
                    Function::Square if transformed_arguments.len() == 1 => Some(transformed_arguments[0] * transformed_arguments[0]),
                    Function::Power if transformed_arguments.len() == 2 => Some(transformed_arguments[0].powf(transformed_arguments[1])),
                    Function::Exponential if transformed_arguments.len() == 1 => Some(transformed_arguments[0].exp()),
                    Function::LogE if transformed_arguments.len() == 1 && transformed_arguments[0] > 0.0 => Some(transformed_arguments[0].ln()),
                    Function::LogBase if transformed_arguments.len() == 2 && transformed_arguments[0] > 0.0 && transformed_arguments[1] > 0.0 => Some(transformed_arguments[0].log(transformed_arguments[1])),
                    Function::Sin if transformed_arguments.len() == 1 => Some(transformed_arguments[0].sin()),
                    Function::Cos if transformed_arguments.len() == 1 => Some(transformed_arguments[0].cos()),
                    Function::Tan if transformed_arguments.len() == 1 => Some(transformed_arguments[0].tan()),
                    _ => None
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum FilterExpression {
    Transform { expression: TransformExpression },
    Compare { operation: CompareOperation, left: Box<FilterExpression>, right: Box<FilterExpression> },
    And { left: Box<FilterExpression>, right: Box<FilterExpression> },
    Or { left: Box<FilterExpression>, right: Box<FilterExpression> }
}

impl FilterExpression {
    pub fn input_value() -> FilterExpression {
        FilterExpression::Transform { expression: TransformExpression::InputValue }
    }

    pub fn value(value: f64) -> FilterExpression {
        FilterExpression::Transform { expression: TransformExpression::Value { value } }
    }

    pub fn evaluate(&self, input: &ExpressionValue) -> Option<bool> {
        self.evaluate_internal(input)?.bool()
    }

    fn evaluate_internal(&self, input: &ExpressionValue) -> Option<FilterExpressionResult> {
        match self {
            FilterExpression::Transform { expression } => {
                Some(FilterExpressionResult::Float(expression.evaluate(input)?))
            }
            FilterExpression::Compare { operation, left, right } => {
                let left = left.evaluate_internal(input)?.float()?;
                let right = right.evaluate_internal(input)?.float()?;

                match operation {
                    CompareOperation::Equal => Some(FilterExpressionResult::Bool(left == right)),
                    CompareOperation::NotEqual => Some(FilterExpressionResult::Bool(left != right)),
                    CompareOperation::GreaterThan => Some(FilterExpressionResult::Bool(left > right)),
                    CompareOperation::GreaterThanOrEqual => Some(FilterExpressionResult::Bool(left >= right)),
                    CompareOperation::LessThan => Some(FilterExpressionResult::Bool(left > right)),
                    CompareOperation::LessThanOrEqual => Some(FilterExpressionResult::Bool(left <= right))
                }
            }
            FilterExpression::And { left, right } => {
                Some(FilterExpressionResult::Bool(left.evaluate_internal(input)?.bool()? && right.evaluate_internal(input)?.bool()?))
            }
            FilterExpression::Or { left, right } => {
                Some(FilterExpressionResult::Bool(left.evaluate_internal(input)?.bool()? || right.evaluate_internal(input)?.bool()?))
            }
        }
    }
}

enum FilterExpressionResult {
    Float(f64),
    Bool(bool)
}

impl FilterExpressionResult {
    pub fn bool(&self) -> Option<bool> {
        match self {
            FilterExpressionResult::Float(_) => None,
            FilterExpressionResult::Bool(value) => Some(*value)
        }
    }

    pub fn float(&self) -> Option<f64> {
        match self {
            FilterExpressionResult::Float(value) => Some(*value),
            FilterExpressionResult::Bool(_) => None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ArithmeticOperation {
    Add,
    Subtract,
    Multiply,
    Divide
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Function {
    Abs,
    Max,
    Min,
    Round,
    Ceil,
    Floor,
    Sqrt,
    Square,
    Power,
    Exponential,
    LogE,
    LogBase,
    Sin,
    Cos,
    Tan
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CompareOperation {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

#[test]
fn test_transform1() {
    let expression = TransformExpression::Arithmetic {
        operation: ArithmeticOperation::Multiply,
        left: Box::new(TransformExpression::InputValue),
        right: Box::new(TransformExpression::InputValue)
    };

    assert_eq!(Some(16.0), expression.evaluate(&ExpressionValue::Float(4.0)));
}

#[test]
fn test_transform2() {
    let expression = TransformExpression::Arithmetic {
        operation: ArithmeticOperation::Add,
        left: Box::new(TransformExpression::InputValue),
        right: Box::new(TransformExpression::Function { function: Function::Sqrt, arguments: vec![TransformExpression::InputValue] })
    };

    assert_eq!(Some(4.0 + 4.0f64.sqrt()), expression.evaluate(&ExpressionValue::Float(4.0)));
}


#[test]
fn test_filter1() {
    let expression = FilterExpression::Compare {
        operation: CompareOperation::GreaterThan,
        left: Box::new(FilterExpression::Transform { expression: TransformExpression::InputValue }),
        right: Box::new(FilterExpression::Transform { expression: TransformExpression::Value { value: 0.7 } })
    };

    assert_eq!(Some(true), expression.evaluate(&ExpressionValue::Float(0.9)));
    assert_eq!(Some(false), expression.evaluate(&ExpressionValue::Float(0.6)));
}