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

    pub fn numerator(&self) -> Option<f64> {
        match self {
            ExpressionValue::Ratio(ratio) => Some(ratio.numerator() as f64),
            _ => None
        }
    }

    pub fn denominator(&self) -> Option<f64> {
        match self {
            ExpressionValue::Ratio(ratio) => Some(ratio.denominator() as f64),
            _ => None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TransformExpression {
    InputValue,
    InputNumerator,
    InputDenominator,
    Value(f64),
    Arithmetic { operation: ArithmeticOperation, left: Box<TransformExpression>, right: Box<TransformExpression> },
    Function { function: Function, arguments: Vec<TransformExpression> }
}

impl TransformExpression {
    pub fn evaluate(&self, input: &ExpressionValue) -> Option<f64> {
        match self {
            TransformExpression::InputValue => input.float(),
            TransformExpression::InputNumerator => input.numerator(),
            TransformExpression::InputDenominator => input.denominator(),
            TransformExpression::Value(value) => Some(*value),
            TransformExpression::Arithmetic { operation, left, right } => {
                let left = left.evaluate(input)?;
                let right = right.evaluate(input)?;
                Some(operation.apply(left, right))
            }
            TransformExpression::Function { function, arguments } => {
                let mut transformed_arguments = Vec::new();
                for argument in arguments {
                    transformed_arguments.push(argument.evaluate(input)?);
                }

                function.apply(&transformed_arguments)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum FilterExpression {
    Value(TransformExpression),
    Compare { operation: CompareOperation, left: Box<FilterExpression>, right: Box<FilterExpression> },
    And { left: Box<FilterExpression>, right: Box<FilterExpression> },
    Or { left: Box<FilterExpression>, right: Box<FilterExpression> }
}

impl FilterExpression {
    pub fn input_value() -> FilterExpression {
        FilterExpression::Value(TransformExpression::InputValue)
    }

    pub fn value(value: f64) -> FilterExpression {
        FilterExpression::Value(TransformExpression::Value(value))
    }

    pub fn evaluate(&self, input: &ExpressionValue) -> Option<bool> {
        self.evaluate_internal(input)?.bool()
    }

    fn evaluate_internal(&self, input: &ExpressionValue) -> Option<FilterExpressionResult> {
        match self {
            FilterExpression::Value(expression) => {
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

impl ArithmeticOperation {
    pub fn apply(&self, left: f64, right: f64) -> f64 {
        match self {
            ArithmeticOperation::Add => left + right,
            ArithmeticOperation::Subtract => left - right,
            ArithmeticOperation::Multiply => left * right,
            ArithmeticOperation::Divide => left / right
        }
    }
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

impl Function {
    pub fn apply(&self, arguments: &[f64]) -> Option<f64> {
        match self {
            Function::Abs if arguments.len() == 1 => Some(arguments[0].abs()),
            Function::Max if arguments.len() == 2 => Some(arguments[0].max(arguments[1])),
            Function::Min if arguments.len() == 2 => Some(arguments[0].min(arguments[1])),
            Function::Round if arguments.len() == 1 => Some(arguments[0].round()),
            Function::Ceil if arguments.len() == 1 => Some(arguments[0].ceil()),
            Function::Floor if arguments.len() == 1 => Some(arguments[0].floor()),
            Function::Sqrt if arguments.len() == 1 && arguments[0] >= 0.0 => Some(arguments[0].sqrt()),
            Function::Square if arguments.len() == 1 => Some(arguments[0] * arguments[0]),
            Function::Power if arguments.len() == 2 => Some(arguments[0].powf(arguments[1])),
            Function::Exponential if arguments.len() == 1 => Some(arguments[0].exp()),
            Function::LogE if arguments.len() == 1 && arguments[0] > 0.0 => Some(arguments[0].ln()),
            Function::LogBase if arguments.len() == 2 && arguments[0] > 0.0 && arguments[1] > 0.0 => Some(arguments[0].log(arguments[1])),
            Function::Sin if arguments.len() == 1 => Some(arguments[0].sin()),
            Function::Cos if arguments.len() == 1 => Some(arguments[0].cos()),
            Function::Tan if arguments.len() == 1 => Some(arguments[0].tan()),
            _ => None
        }
    }
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
        left: Box::new(FilterExpression::Value(TransformExpression::InputValue)),
        right: Box::new(FilterExpression::Value(TransformExpression::Value(0.7)))
    };

    assert_eq!(Some(true), expression.evaluate(&ExpressionValue::Float(0.9)));
    assert_eq!(Some(false), expression.evaluate(&ExpressionValue::Float(0.6)));
}