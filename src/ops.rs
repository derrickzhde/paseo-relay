use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

const JSON: &str = "application/json";
const PROMETHEUS: &str = "text/plain; version=0.0.4";

pub async fn health() -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, JSON)], r#"{"status":"ok"}"#).into_response()
}

pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    if state.ready() {
        (StatusCode::OK, [(header::CONTENT_TYPE, JSON)], r#"{"status":"ready"}"#).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, JSON)],
            r#"{"status":"unready"}"#,
        )
            .into_response()
    }
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let body = state.metrics.render(state.ready(), state.draining(), state.rooms.len());
    (StatusCode::OK, [(header::CONTENT_TYPE, PROMETHEUS)], body).into_response()
}

pub async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, [(header::CONTENT_TYPE, "text/plain")], "not found\n").into_response()
}
