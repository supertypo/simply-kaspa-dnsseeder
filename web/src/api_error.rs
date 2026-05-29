use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug)]
pub(crate) enum ApiError {
    BadRequest(&'static str),
    NotFound(&'static str),
    Unauthorized(&'static str),
    #[allow(dead_code)]
    Forbidden(&'static str),
    RateLimited(&'static str),
    BadGateway(String),
    Internal(&'static str),
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::BadGateway(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::BadRequest(m)
            | Self::NotFound(m)
            | Self::Unauthorized(m)
            | Self::Forbidden(m)
            | Self::RateLimited(m)
            | Self::Internal(m) => m,
            Self::BadGateway(m) => m,
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody<'a> {
    status: u16,
    error: &'a str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ApiErrorBody {
            status: status.as_u16(),
            error: self.message(),
        };
        (status, Json(body)).into_response()
    }
}
