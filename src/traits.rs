use crate::metric::expression::ExpressionValue;
use crate::metric::ratio::Ratio;

pub trait MinMax {
    fn min(&self, other: Self) -> Self;
    fn max(&self, other: Self) -> Self;
}

impl MinMax for f64 {
    fn min(&self, other: Self) -> Self {
        f64::min(*self, other)
    }

    fn max(&self, other: Self) -> Self {
        f64::max(*self, other)
    }
}

impl MinMax for f32 {
    fn min(&self, other: Self) -> Self {
        f32::min(*self, other)
    }

    fn max(&self, other: Self) -> Self {
        f32::max(*self, other)
    }
}

impl MinMax for u32 {
    fn min(&self, other: Self) -> Self {
        if self < &other {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        if self > &other {
            *self
        } else {
            other
        }
    }
}

pub trait ToExpressionValue {
    fn to_value(&self) -> ExpressionValue;
}

impl ToExpressionValue for f64 {
    fn to_value(&self) -> ExpressionValue {
        ExpressionValue::Float(*self)
    }
}

impl ToExpressionValue for Ratio {
    fn to_value(&self) -> ExpressionValue {
        ExpressionValue::Ratio(*self)
    }
}