use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc};
use std::time::Duration;

use serde_json::json;
use serde::Deserialize;

use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::http::StatusCode;
use axum::routing::{post, put};
use tokio::time;

use crate::{AddCountValue, MetricsEngine, PrimaryTag, Query, TimeRange};
use crate::engine::{AddGaugeValue, MetricsEngineError};

pub async fn main() {
    let app_state = Arc::new(AppState::new());
    let app = Router::with_state(app_state.clone())
        .route("/metrics/gauge", post(create_gauge_metric))
        .route("/metrics/gauge/:name", put(add_gauge_metric_value))

        .route("/metrics/count", post(create_count_metric))
        .route("/metrics/count/:name", put(add_count_metric_value))

        .route("/metrics/query/:name", post(metric_query))
        .route("/metrics/primary-tag/:name", post(add_primary_tag))
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

pub type ServerResult<T> = Result<T, MetricsEngineError>;

impl IntoResponse for MetricsEngineError {
    fn into_response(self) -> Response {
        let (error_code, error_message) = match self {
            MetricsEngineError::FailedToCreateBaseDir(err) => (StatusCode::BAD_REQUEST, format!("Failed to create base dir due to: {}", err)),
            MetricsEngineError::FailedToLoadMetricDefinitions(err) => (StatusCode::BAD_REQUEST, format!("Failed to load metrics definitions due to: {}", err)),
            MetricsEngineError::FailedToSaveMetricDefinitions(err) => (StatusCode::BAD_REQUEST, format!("Failed to save metrics definitions due to: {}", err)),
            MetricsEngineError::MetricAlreadyExists => (StatusCode::BAD_REQUEST, format!("Metrics already exist.")),
            MetricsEngineError::MetricNotFound => (StatusCode::NOT_FOUND, format!("Metrics not found.")),
            MetricsEngineError::WrongMetricType => (StatusCode::BAD_REQUEST, format!("Wrong metric type.")),
            MetricsEngineError::UndefinedOperation => (StatusCode::BAD_REQUEST, format!("Operation not defined for current metric type.")),
            MetricsEngineError::InvalidQueryInput => (StatusCode::BAD_REQUEST, format!("Invalid query input.")),
            MetricsEngineError::Metric(err) => (StatusCode::BAD_REQUEST, format!("Metric error: {:?}", err))
        };

        let mut response = Json(
            json!({
                "message": error_message
            })
        ).into_response();
        *response.status_mut() = error_code;

        response
    }
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
struct CreateMetric {
    name: String
}

async fn create_gauge_metric(State(state): State<Arc<AppState>>, Json(input): Json<CreateMetric>) -> ServerResult<Response> {
    state.metrics_engine.add_gauge_metric(&input.name)?;
    Ok(Json(json!({})).into_response())
}

async fn create_count_metric(State(state): State<Arc<AppState>>, Json(input): Json<CreateMetric>) -> ServerResult<Response> {
    state.metrics_engine.add_count_metric(&input.name)?;
    Ok(Json(json!({})).into_response())
}

#[derive(Deserialize)]
struct AddPrimaryTag {
    tag: String
}

async fn add_primary_tag(State(state): State<Arc<AppState>>,
                         Path(name): Path<String>,
                         Json(primary_tag): Json<AddPrimaryTag>) -> ServerResult<Response> {
    state.metrics_engine.add_primary_tag(&name, PrimaryTag::Named(primary_tag.tag))?;
    Ok(Json(json!({})).into_response())
}

async fn add_gauge_metric_value(State(state): State<Arc<AppState>>,
                                Path(name): Path<String>,
                                Json(metric_values): Json<Vec<AddGaugeValue>>) -> ServerResult<Response> {
    let num_inserted = state.metrics_engine.gauge(&name, metric_values.into_iter())?;
    Ok(
        Json(
            json!({
                "num_inserted": num_inserted
            })
        ).into_response()
    )
}

async fn add_count_metric_value(State(state): State<Arc<AppState>>,
                                Path(name): Path<String>,
                                Json(metric_values): Json<Vec<AddCountValue>>) -> ServerResult<Response> {
    let num_inserted = state.metrics_engine.count(&name, metric_values.into_iter())?;
    Ok(
        Json(
            json!({
                "num_inserted": num_inserted
            })
        ).into_response()
    )
}

#[derive(Deserialize)]
enum MetricOperation {
    Average,
    Sum,
    Max,
    Percentile
}

#[derive(Deserialize)]
struct MetricQuery {
    operation: MetricOperation,
    percentile: Option<i32>,
    duration: Option<f64>,
    start: f64,
    end: f64
}

async fn metric_query(State(state): State<Arc<AppState>>,
                      Path(name): Path<String>,
                      Json(input_query): Json<MetricQuery>) -> ServerResult<Response> {
    let query = Query::new(TimeRange::new(input_query.start, input_query.end));
    let value = match input_query.operation {
        MetricOperation::Average => {
            if let Some(duration) = input_query.duration {
                state.metrics_engine.average_in_window(&name, query, Duration::from_secs_f64(duration)).map(|x| json!(x))
            } else {
                state.metrics_engine.average(&name, query).map(|x| json!(x))
            }
        },
        MetricOperation::Sum => {
            if let Some(duration) = input_query.duration {
                state.metrics_engine.sum_in_window(&name, query, Duration::from_secs_f64(duration)).map(|x| json!(x))
            } else {
                state.metrics_engine.sum(&name, query).map(|x| json!(x))
            }
        },
        MetricOperation::Max => {
            if let Some(duration) = input_query.duration {
                state.metrics_engine.max_in_window(&name, query, Duration::from_secs_f64(duration)).map(|x| json!(x))
            } else {
                state.metrics_engine.max(&name, query).map(|x| json!(x))
            }
        },
        MetricOperation::Percentile => {
            if let Some(percentile) = input_query.percentile {
                if let Some(duration) = input_query.duration {
                    state.metrics_engine.percentile_in_window(&name, query, Duration::from_secs_f64(duration), percentile).map(|x| json!(x))
                } else {
                    state.metrics_engine.percentile(&name, query, percentile).map(|x| json!(x))
                }
            } else {
                return Err(MetricsEngineError::InvalidQueryInput);
            }
        }
    }?;

    Ok(
        Json(
            json!({
                "value": value
            })
        ).into_response()
    )
}