use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use serde::Deserialize;

use tokio::time;

use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::http::StatusCode;
use axum::routing::{post, put};

use crate::engine::MetricsEngine;
use crate::engine::io::{AddCountValue, AddGaugeValue, AddRatioValue, MetricsEngineError};
use crate::engine::querying::{MetricQuery, MetricQueryExpression};
use crate::metric::common::{MetricConfig, MetricType, MetricStorageDurationConfig};
use crate::metric::OperationResult;
use crate::metric::tags::{PrimaryTag, Tag};
use crate::model::{TimeRange};

pub async fn main() {
    let app_state = Arc::new(AppState::new());
    let app = Router::with_state(app_state.clone())
        .route("/metrics/gauge", post(create_gauge_metric))
        .route("/metrics/gauge/:name", put(add_gauge_metric_value))

        .route("/metrics/count", post(create_count_metric))
        .route("/metrics/count/:name", put(add_count_metric_value))

        .route("/metrics/ratio", post(create_ratio_metric))
        .route("/metrics/ratio/:name", put(add_ratio_metric_value))

        .route("/metrics/query", post(metric_query))

        .route("/metrics/primary-tag/:name", post(add_primary_tag))
        .route("/metrics/auto-primary-tag/:name", post(add_auto_primary_tag))
    ;

    tokio::spawn(async move {
        let mut duration = time::interval(Duration::from_secs_f64(0.25));
        loop {
            duration.tick().await;
            app_state.metrics_engine.scheduled();
        }
    });

    let address = SocketAddr::new(Ipv4Addr::from_str("127.0.0.1").unwrap().into(), 9090);
    println!("Listening on {}", address);
    tokio::select! {
        result = axum::Server::bind(&address).serve(app.into_make_service()) => {
            result.unwrap();
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
            return;
        }
    }
}

pub type ServerResult<T> = Result<T, MetricsEngineError>;

impl IntoResponse for MetricsEngineError {
    fn into_response(self) -> Response {
        let (status_code, error_message) = match self {
            MetricsEngineError::FailedToCreateBaseDir(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create base dir due to: {}", err)),
            MetricsEngineError::FailedToLoadMetricDefinitions(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load metrics definitions due to: {}", err)),
            MetricsEngineError::FailedToSaveMetricDefinitions(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to save metrics definitions due to: {}", err)),
            MetricsEngineError::MetricAlreadyExists => (StatusCode::BAD_REQUEST, format!("Metrics already exist.")),
            MetricsEngineError::MetricNotFound => (StatusCode::NOT_FOUND, format!("Metric not found.")),
            MetricsEngineError::WrongMetricType => (StatusCode::BAD_REQUEST, format!("Wrong metric type.")),
            MetricsEngineError::UnexpectedResult => (StatusCode::BAD_REQUEST, format!("Unexpected result.")),
            MetricsEngineError::Metric(err) => (StatusCode::BAD_REQUEST, format!("Metric error: {:?}", err))
        };

        with_response_code(
            Json(
                json!({
                    "message": error_message
                })
            ).into_response(),
            status_code
        )
    }
}

struct AppState {
    metrics_engine: MetricsEngine
}

impl AppState {
    pub fn new() -> AppState {
        AppState {
            metrics_engine: MetricsEngine::new_or_from_existing(std::path::Path::new("server_storage")).unwrap()
        }
    }
}

#[derive(Deserialize)]
struct CreateMetric {
    name: String,
    datapoint_duration: Option<f64>,
    data_keep_time: Option<f64>,
    faster_duration: Option<FasterDuration>
}

#[derive(Deserialize)]
struct FasterDuration {
    datapoint_duration: f64,
    data_keep_time: f64,
}

async fn create_gauge_metric(State(state): State<Arc<AppState>>, Json(input): Json<CreateMetric>) -> ServerResult<Response> {
    create_metric(state, input, MetricType::Gauge)
}

async fn create_count_metric(State(state): State<Arc<AppState>>, Json(input): Json<CreateMetric>) -> ServerResult<Response> {
    create_metric(state, input, MetricType::Count)
}

async fn create_ratio_metric(State(state): State<Arc<AppState>>, Json(input): Json<CreateMetric>) -> ServerResult<Response> {
    create_metric(state, input, MetricType::Ratio)
}

fn create_metric(state: Arc<AppState>, input: CreateMetric, metric_type: MetricType) -> ServerResult<Response> {
    let mut config = MetricConfig::new(metric_type.clone());
    if let Some(datapoint_duration) = input.datapoint_duration {
        config.durations[0].datapoint_duration = datapoint_duration;
    }

    if let Some(data_keep_time) = input.data_keep_time {
        config.durations[0].set_max_segments(data_keep_time);
    }

    if let Some(faster_duration) = input.faster_duration {
        let mut duration = MetricStorageDurationConfig::default_for(metric_type.clone());
        duration.datapoint_duration = faster_duration.datapoint_duration;
        duration.set_max_segments(faster_duration.data_keep_time);
        config.durations.push(duration);
    }

    state.metrics_engine.add_metric_with_config(&input.name, metric_type, config)?;
    Ok(Json(json!({})).into_response())
}

#[derive(Deserialize)]
struct AddPrimaryTag {
    tag: Tag
}

async fn add_primary_tag(State(state): State<Arc<AppState>>,
                         Path(name): Path<String>,
                         Json(primary_tag): Json<AddPrimaryTag>) -> ServerResult<Response> {
    state.metrics_engine.add_primary_tag(&name, PrimaryTag::Named(primary_tag.tag))?;
    Ok(Json(json!({})).into_response())
}

#[derive(Deserialize)]
struct AddAutoPrimaryTag {
    key: String
}

async fn add_auto_primary_tag(State(state): State<Arc<AppState>>,
                         Path(name): Path<String>,
                         Json(primary_tag): Json<AddAutoPrimaryTag>) -> ServerResult<Response> {
    state.metrics_engine.add_auto_primary_tag(&name, &primary_tag.key)?;
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

async fn add_ratio_metric_value(State(state): State<Arc<AppState>>,
                                Path(name): Path<String>,
                                Json(metric_values): Json<Vec<AddRatioValue>>) -> ServerResult<Response> {
    let num_inserted = state.metrics_engine.ratio(&name, metric_values.into_iter())?;
    Ok(
        Json(
            json!({
                "num_inserted": num_inserted
            })
        ).into_response()
    )
}

#[derive(Deserialize)]
struct InputMetricQuery {
    time_range: TimeRange,
    duration: Option<f64>,
    expression: MetricQueryExpression
}

async fn metric_query(State(state): State<Arc<AppState>>,
                      Json(input_query): Json<InputMetricQuery>) -> ServerResult<Response> {
    let query = MetricQuery::new(input_query.time_range, input_query.expression);

    let value = if let Some(duration) = input_query.duration {
        state.metrics_engine.query_in_window(query, Duration::from_secs_f64(duration))?
    } else {
        state.metrics_engine.query(query)?
    };

    operation_result_response(value)
}

fn operation_result_response(value: OperationResult) -> ServerResult<Response> {
    if let Some(error_message) = value.error_message() {
        return Ok(with_response_code(
            Json(
                json!({
                    "message": error_message
                })
            ).into_response(),
            StatusCode::BAD_REQUEST
        ));
    }

    let value = value.as_json();

    Ok(
        Json(
            json!({
                "value": value
            })
        ).into_response()
    )
}

fn with_response_code(mut response: Response, code: StatusCode) -> Response {
    *response.status_mut() = code;
    response
}