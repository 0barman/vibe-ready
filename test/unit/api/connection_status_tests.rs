fn assert_transition(
    from: VibeConnectionStatus,
    to: VibeConnectionStatus,
    expected: Result<VibeConnectionStatus, VibeEngineErrorCode>,
) {
    let mut status = from;
    match expected {
        Ok(value) => {
            assert_eq!(status.trans(to).expect("transition should be valid"), value);
            assert_eq!(status, value);
        }
        Err(code) => {
            let error = status.trans(to).expect_err("transition should be invalid");
            assert_eq!(error.code(), code.code());
            assert_eq!(status, from, "invalid transitions must not mutate status");
        }
    }
}

#[test]
fn transition_matrix_covers_all_status_pairs() {
    use VibeConnectionStatus::*;
    assert_transition(Idle, Idle, Ok(Idle));
    assert_transition(Idle, Connecting, Ok(Connecting));
    assert_transition(Idle, Connected, Err(VibeEngineErrorCode::InternalError));
    assert_transition(Idle, Disconnecting, Err(VibeEngineErrorCode::ConnectionClosed));
    assert_transition(Idle, Disconnected, Ok(Disconnected));

    assert_transition(Connecting, Idle, Ok(Idle));
    assert_transition(Connecting, Connecting, Err(VibeEngineErrorCode::InternalError));
    assert_transition(Connecting, Connected, Ok(Connected));
    assert_transition(Connecting, Disconnecting, Ok(Disconnecting));
    assert_transition(Connecting, Disconnected, Ok(Disconnected));

    assert_transition(Connected, Idle, Ok(Idle));
    assert_transition(Connected, Connecting, Err(VibeEngineErrorCode::ConnectionExists));
    assert_transition(Connected, Connected, Err(VibeEngineErrorCode::InternalError));
    assert_transition(Connected, Disconnecting, Ok(Disconnecting));
    assert_transition(Connected, Disconnected, Ok(Disconnected));

    assert_transition(Disconnecting, Idle, Ok(Idle));
    assert_transition(Disconnecting, Connecting, Ok(Connecting));
    assert_transition(Disconnecting, Connected, Err(VibeEngineErrorCode::InternalError));
    assert_transition(Disconnecting, Disconnecting, Err(VibeEngineErrorCode::ConnectionClosing));
    assert_transition(Disconnecting, Disconnected, Ok(Disconnected));

    assert_transition(Disconnected, Idle, Ok(Idle));
    assert_transition(Disconnected, Connecting, Ok(Connecting));
    assert_transition(Disconnected, Connected, Err(VibeEngineErrorCode::InternalError));
    assert_transition(Disconnected, Disconnecting, Err(VibeEngineErrorCode::ConnectionClosed));
    assert_transition(Disconnected, Disconnected, Ok(Disconnected));
}

#[test]
fn display_and_default_are_stable() {
    assert_eq!(VibeConnectionStatus::default(), VibeConnectionStatus::Idle);
    assert_eq!(VibeConnectionStatus::Connected.to_string(), "Connected");
    assert_eq!(VibeConnectionStatus::Disconnecting.to_string(), "Disconnecting");
}
