import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from lab_link import LabSync, LanPassphraseAuth, ReactiveModel


class ProtectedState(ReactiveModel):
    value: int = 7


def protected_app(**auth_options):
    auth = LanPassphraseAuth(
        "TEST-PASS-CODE",
        trust_loopback=False,
        **auth_options,
    )
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())
    return sync.create_app(), auth


def test_state_and_snapshot_are_protected_before_authentication():
    app, _ = protected_app()
    with TestClient(app) as client:
        assert client.get("/sync/state").status_code == 401
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/sync/ws"):
                pass
        assert exc_info.value.code == 4401


def test_passphrase_creates_per_browser_session_and_logout_revokes_only_it():
    app, _ = protected_app()
    with TestClient(app) as first, TestClient(app) as second:
        for client in (first, second):
            response = client.post(
                "/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"}
            )
            assert response.status_code == 200
            assert response.json() == {"authorized": True}
            assert client.get("/sync/state").json() == {"value": 7}

        first.post("/sync/auth/logout")
        assert first.get("/sync/state").status_code == 401
        assert second.get("/sync/state").status_code == 200


def test_invite_is_single_use_and_opens_an_authenticated_websocket():
    app, auth = protected_app()
    invite = auth.create_invite()
    with TestClient(app) as first, TestClient(app) as second:
        response = first.post("/sync/auth/invite", json={"invite": invite.token})
        assert response.status_code == 200
        assert invite.token not in response.text
        with first.websocket_connect("/sync/ws") as websocket:
            assert websocket.receive_json()["type"] == "snapshot"

        reused = second.post("/sync/auth/invite", json={"invite": invite.token})
        assert reused.status_code == 401
        assert reused.json()["error"] == "invalid_or_expired_invite"


def test_expired_invite_is_rejected():
    app, auth = protected_app()
    invite = auth.create_invite(ttl=0.001)
    import time

    time.sleep(0.01)
    with TestClient(app) as client:
        assert client.post(
            "/sync/auth/invite", json={"invite": invite.token}
        ).status_code == 401


def test_expired_session_cannot_continue_sending_websocket_commands():
    app, _ = protected_app(session_ttl=0.001)
    import time

    with TestClient(app) as client:
        client.post("/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"})
        with client.websocket_connect("/sync/ws") as websocket:
            websocket.receive_json()
            time.sleep(0.01)
            websocket.send_json({"type": "stream_resync", "id": "missing"})
            with pytest.raises(WebSocketDisconnect) as exc_info:
                websocket.receive_json()
            assert exc_info.value.code == 4401


def test_cross_origin_login_and_websocket_are_rejected():
    app, _ = protected_app()
    headers = {"origin": "https://attacker.example"}
    with TestClient(app) as client:
        response = client.post(
            "/sync/auth/login",
            json={"passphrase": "TEST-PASS-CODE"},
            headers=headers,
        )
        assert response.status_code == 403
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/sync/ws", headers=headers):
                pass
        assert exc_info.value.code == 4401


def test_rate_limits_repeated_bad_passphrases():
    app, _ = protected_app(max_failures=2)
    with TestClient(app) as client:
        for _ in range(2):
            assert client.post(
                "/sync/auth/login", json={"passphrase": "wrong"}
            ).status_code == 401
        assert client.post(
            "/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"}
        ).status_code == 429


def test_same_ip_origin_is_allowed():
    app, _ = protected_app()
    with TestClient(app) as client:
        response = client.post(
            "/sync/auth/login",
            json={"passphrase": "TEST-PASS-CODE"},
            headers={"origin": "http://127.0.0.1", "host": "127.0.0.1"},
        )
        assert response.status_code == 200


def test_named_same_host_origin_requires_explicit_allow_list():
    app, _ = protected_app()
    with TestClient(app) as client:
        response = client.post(
            "/sync/auth/login",
            json={"passphrase": "TEST-PASS-CODE"},
            headers={"origin": "http://instrument.example", "host": "instrument.example"},
        )
        assert response.status_code == 403


def test_explicitly_allowed_named_origin_is_accepted():
    app, _ = protected_app(allowed_origins={"https://instrument.example"})
    with TestClient(app) as client:
        response = client.post(
            "/sync/auth/login",
            json={"passphrase": "TEST-PASS-CODE"},
            headers={"origin": "https://instrument.example"},
        )
        assert response.status_code == 200
