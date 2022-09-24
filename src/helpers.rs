pub enum TimeMeasurementUnit {
    Seconds,
    Milliseconds,
    Microseconds
}

pub struct TimeMeasurement {
    pattern: String,
    start_time: std::time::Instant,
    unit: TimeMeasurementUnit
}

impl TimeMeasurement {
    pub fn new(pattern: &str, unit: TimeMeasurementUnit) -> TimeMeasurement {
        TimeMeasurement {
            pattern: pattern.to_owned(),
            start_time: std::time::Instant::now(),
            unit
        }
    }

    pub fn elapsed_seconds(&self) -> f64 {
        return (std::time::Instant::now() - self.start_time).as_nanos() as f64 / 1.0E9
    }

    pub fn elapsed_ms(&self) -> f64 {
        return (std::time::Instant::now() - self.start_time).as_nanos() as f64 / 1.0E6
    }

    pub fn elapsed_micro(&self) -> f64 {
        return (std::time::Instant::now() - self.start_time).as_nanos() as f64 / 1.0E3
    }
}

impl Drop for TimeMeasurement {
    fn drop(&mut self) {
        match self.unit {
            TimeMeasurementUnit::Seconds => {
                println!("{}: {:.2} s", self.pattern, self.elapsed_seconds())
            }
            TimeMeasurementUnit::Milliseconds => {
                println!("{}: {:.2} ms", self.pattern, self.elapsed_ms())
            }
            TimeMeasurementUnit::Microseconds => {
                println!("{}: {:.2} μs", self.pattern, self.elapsed_micro())
            }
        }
    }
}