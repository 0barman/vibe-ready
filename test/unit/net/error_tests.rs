use crate::api::engine_error::VibeEngineErrorCode;
use crate::VibeErrorKind;

#[test]
fn status_error_maps_4xx_to_bad_request() {
    let err = status_error("http GET http://x", 404);
    assert_eq!(err.code(), VibeEngineErrorCode::BadRequest.code());
    assert_eq!(err.kind(), VibeErrorKind::Network);
    assert_eq!(err.source_message(), Some("http status 404"));
    assert_eq!(err.context(), &["http GET http://x".to_string()]);
}

#[test]
fn status_error_maps_5xx_to_internal_server_error() {
    let err = status_error("ctx", 503);
    assert_eq!(err.code(), VibeEngineErrorCode::InternalServerError.code());
}

#[test]
fn serialize_error_maps_to_serde_serialize() {
    let json_err = serde_json::from_str::<i32>("not json").unwrap_err();
    let err = serialize_error("ctx", json_err);
    assert_eq!(err.code(), VibeEngineErrorCode::SerdeSerializeError.code());
}
