use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc};
use std::time::Duration;

use serde_json::json;
use serde::Deserialize;

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::{Json, Router};
use axum::routing::{post, put};
use tokio::time;

use crate::{MetricsEngine, Query, TimeRange};

pub async fn main() {
    let app_state = Arc::new(AppState::new());
    let app = Router::with_state(app_state.clone())
        .route("/metrics/gauge", post(add_gauge_metric))
        .route("/metrics/gauge/:name", put(add_gauge_metric_value))
        .route("/metrics/count", post(add_count_metric))
        .route("/metrics/query/:name", post(metric_query))
    ;

    tokio::spawn(async move {
        let mut duration = time::interval(Duration::from_secs_f64(1.0));
        loop {
            duration.tick().await;
            app_state.metrics_engine.scheduled();
        }
    });

    let address = SocketAddr::new(Ipv4Addr::from_str("127.0.0.1").unwrap().into(), 9090);
    println!("Listing on {}", address);
    axum::Server::bind(&address)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

struct AppState {
    metrics_engine: MetricsEngine
}

impl AppState {
    pub fn new() -> AppState {
        AppState {
            metrics_engine: MetricsEngine::new_or_from_existing(std::path::Path::new("test_metric_server")).unwrap()
        }
    }
}

#[derive(Deserialize)]
struct AddMetric {
    name: String
}

async fn add_gauge_metric(State(state): State<Arc<AppState>>,
                          Json(input): Json<AddMetric>) -> impl IntoResponse {
    let success = state.metrics_engine.add_gauge_metric(&input.name).is_ok();
    Json(
        json!({
            "success": success
        })
    )
}

async fn add_count_metric(State(state): State<Arc<AppState>>,
                          Json(input): Json<AddMetric>) -> impl IntoResponse {
    let success = state.metrics_engine.add_count_metric(&input.name).is_ok();
    Json(
        json!({
            "success": success
        })
    )
}

#[derive(Deserialize)]
struct AddGaugeMetricValue {
    time: f64,
    value: f64,
    tags: Vec<String>
}

async fn add_gauge_metric_value(State(state): State<Arc<AppState>>,
                                Path(name): Path<String>,
                                Json(metric_values): Json<Vec<AddGaugeMetricValue>>) -> impl IntoResponse {
    for entry in metric_values {
        if !state.metrics_engine.gauge(&name, entry.time, entry.value, entry.tags).is_ok() {
            return Json(
                json!({
                    "success": false
                })
            );
        }
    }

    Json(
        json!({
            "success": true
        })
    )
}

#[derive(Deserialize)]
enum MetricOperation {
    Average,
    Sum
}

#[derive(Deserialize)]
struct MetricQuery {
    operation: MetricOperation,
    start: f64,
    end: f64,
}

async fn metric_query(State(state): State<Arc<AppState>>,
                      Path(name): Path<String>,
                      Json(input_query): Json<MetricQuery>) -> impl IntoResponse {
    let query = Query::new(TimeRange::new(input_query.start, input_query.end));
    let value = match input_query.operation {
        MetricOperation::Average => state.metrics_engine.average(&name, query),
        MetricOperation::Sum => state.metrics_engine.sum(&name, query),
    };

    Json(
        json!({
            "value": value
        })
    )
}