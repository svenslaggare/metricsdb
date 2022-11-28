# MetricsDB
Metrics / time series database written in Rust.

Features:
* Metric types: gauge (instantaneous value), count and ratio (two counts).
* Tagging of values are split into two types: primary and secondary tags. Primary tags control how data is stored while secondary tags are bit sets.
* Allows storing data at different granularities.