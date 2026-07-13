import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from lab_link import (
    CommandContext,
    LabSync,
    LanPassphraseAuth,
    ReactiveModel,
    SQLiteAuthStore,
)


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
            assert response.json()["authorized"] is True
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
        assert (
            client.post("/sync/auth/invite", json={"invite": invite.token}).status_code
            == 401
        )


def test_expired_session_cannot_continue_sending_websocket_commands():
    app, _ = protected_app(session_ttl=0.1)
    import time

    with TestClient(app) as client:
        client.post("/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"})
        with client.websocket_connect("/sync/ws") as websocket:
            websocket.receive_json()
            time.sleep(0.15)
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
            assert (
                client.post(
                    "/sync/auth/login", json={"passphrase": "wrong"}
                ).status_code
                == 401
            )
        assert (
            client.post(
                "/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"}
            ).status_code
            == 429
        )


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
            headers={
                "origin": "http://instrument.example",
                "host": "instrument.example",
            },
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


def test_persistent_store_starts_unconfigured_and_setup_is_loopback_only(tmp_path):
    store = SQLiteAuthStore(tmp_path / "auth.db")
    auth = LanPassphraseAuth(store=store, trust_loopback=False)
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())
    app = sync.create_app()

    with TestClient(app) as remote:
        assert remote.get("/sync/auth/status").json() == {
            "configured": False,
            "authorized": False,
            "principal": None,
        }
        assert (
            remote.post(
                "/sync/auth/setup", json={"passphrase": "long-enough-password"}
            ).status_code
            == 403
        )

    with TestClient(app, client=("127.0.0.1", 50000)) as local:
        response = local.post(
            "/sync/auth/setup",
            json={
                "passphrase": "long-enough-password",
                "remember": True,
                "deviceName": "Instrument host",
            },
        )
        assert response.status_code == 200
        assert response.json()["session"]["remembered"] is True
        assert auth.configured is True
        assert auth.passphrase is None


def test_remembered_session_and_passphrase_survive_restart(tmp_path):
    path = tmp_path / "auth.db"
    first_auth = LanPassphraseAuth(
        "long-enough-password",
        store=SQLiteAuthStore(path),
        trust_loopback=False,
    )
    first_sync = LabSync(auth=first_auth)
    first_sync.bind_state(ProtectedState())
    with TestClient(first_sync.create_app()) as client:
        response = client.post(
            "/sync/auth/login",
            json={
                "passphrase": "long-enough-password",
                "remember": True,
                "deviceName": "Andrew's tablet",
            },
        )
        assert response.status_code == 200
        cookie = client.cookies.get(first_auth.cookie_name)
        assert cookie

    second_auth = LanPassphraseAuth(store=SQLiteAuthStore(path), trust_loopback=False)
    assert second_auth.configured is True
    assert second_auth.verify_passphrase("long-enough-password") is True
    second_sync = LabSync(auth=second_auth)
    second_sync.bind_state(ProtectedState())
    with TestClient(
        second_sync.create_app(), cookies={second_auth.cookie_name: cookie}
    ) as restored:
        assert restored.get("/sync/state").status_code == 200
        sessions = second_auth.list_sessions()
        assert sessions[0].label == "Andrew's tablet"
        assert sessions[0].remembered is True

    credential = second_auth.create_api_token(
        "storage test", capabilities={"read_state"}
    )
    database_bytes = b"".join(
        file.read_bytes() for file in tmp_path.glob("auth.db*") if file.is_file()
    )
    assert b"long-enough-password" not in database_bytes
    assert credential.token.encode() not in database_bytes
    assert cookie.encode() not in database_bytes


def test_normal_session_does_not_survive_restart(tmp_path):
    path = tmp_path / "auth.db"
    auth = LanPassphraseAuth(
        "long-enough-password",
        store=SQLiteAuthStore(path),
        trust_loopback=False,
    )
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())
    with TestClient(sync.create_app()) as client:
        client.post("/sync/auth/login", json={"passphrase": "long-enough-password"})
        cookie = client.cookies.get(auth.cookie_name)

    restarted = LanPassphraseAuth(store=SQLiteAuthStore(path), trust_loopback=False)
    restarted_sync = LabSync(auth=restarted)
    restarted_sync.bind_state(ProtectedState())
    with TestClient(
        restarted_sync.create_app(), cookies={restarted.cookie_name: cookie}
    ) as client:
        assert client.get("/sync/state").status_code == 401


def test_invite_lifecycle_reports_consumption_and_expiration():
    app, auth = protected_app()
    events = []
    auth.on_invite_event(events.append)
    consumed = auth.create_invite()
    expired = auth.create_invite(ttl=0.001)

    with TestClient(app) as client:
        assert (
            client.post(
                "/sync/auth/invite", json={"invite": consumed.token}
            ).status_code
            == 200
        )
    import time

    time.sleep(0.01)
    assert auth.invite_status(expired.id) == "expired"
    assert [(event.invite_id, event.status) for event in events] == [
        (consumed.id, "consumed"),
        (expired.id, "expired"),
    ]


def test_runtime_invite_emits_expiration_without_polling():
    app, auth = protected_app()
    events = []
    auth.on_invite_event(events.append)
    import time

    with TestClient(app) as owner:
        owner.post(
            "/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"}
        )
        created = owner.post("/sync/auth/invites", json={"ttl": 0.02}).json()
        time.sleep(0.05)
        assert [(event.invite_id, event.status) for event in events] == [
            (created["id"], "expired")
        ]


def test_invited_session_cannot_mint_more_invitations():
    app, auth = protected_app()
    invite = auth.create_invite()
    with TestClient(app) as invited:
        invited.post("/sync/auth/invite", json={"invite": invite.token})
        assert invited.post("/sync/auth/invites", json={}).status_code == 403

    with TestClient(app) as owner:
        owner.post("/sync/auth/login", json={"passphrase": "TEST-PASS-CODE"})
        created = owner.post("/sync/auth/invites", json={})
        assert created.status_code == 200
        assert created.json()["status"] == "active"


def test_api_token_survives_restart_and_reaches_command_context(tmp_path):
    path = tmp_path / "auth.db"
    first = LanPassphraseAuth(
        "long-enough-password",
        store=SQLiteAuthStore(path),
        trust_loopback=False,
    )
    credential = first.create_api_token(
        "cooldown monitor", capabilities={"control", "read_state"}
    )

    auth = LanPassphraseAuth(store=SQLiteAuthStore(path), trust_loopback=False)
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())

    @sync.command
    def identify(ctx: CommandContext):
        assert ctx.auth is not None
        return {
            "kind": ctx.auth.kind,
            "label": ctx.auth.label,
            "canRead": ctx.auth.can("read_state"),
            "canManage": ctx.auth.can("manage_access"),
        }

    @sync.command(requires={"read_state"})
    def read_identity(ctx: CommandContext):
        return ctx.auth.label if ctx.auth else None

    with TestClient(sync.create_app()) as client:
        headers = {"authorization": f"Bearer {credential.token}"}
        with client.websocket_connect("/sync/ws", headers=headers) as websocket:
            websocket.receive_json()
            websocket.send_json(
                {
                    "type": "command",
                    "command": "identify",
                    "params": {},
                    "requestId": "identify-1",
                }
            )
            response = websocket.receive_json()
            assert response["result"] == {
                "kind": "api_token",
                "label": "cooldown monitor",
                "canRead": True,
                "canManage": False,
            }

    read_only = auth.create_api_token(
        "read-only dashboard", capabilities={"read_state"}
    )
    with TestClient(sync.create_app()) as client:
        headers = {"authorization": f"Bearer {read_only.token}"}
        with client.websocket_connect("/sync/ws", headers=headers) as websocket:
            assert websocket.receive_json()["type"] == "snapshot"
            websocket.send_json(
                {
                    "type": "command",
                    "command": "identify",
                    "params": {},
                    "requestId": "forbidden-1",
                }
            )
            response = websocket.receive_json()
            assert response["type"] == "command_error"
            assert response["code"] == "forbidden"
            websocket.send_json(
                {
                    "type": "command",
                    "command": "read_identity",
                    "params": {},
                    "requestId": "allowed-1",
                }
            )
            response = websocket.receive_json()
            assert response["type"] == "command_ack"
            assert response["result"] == "read-only dashboard"


def test_cross_origin_management_is_rejected_even_for_loopback():
    auth = LanPassphraseAuth("TEST-PASS-CODE", trust_loopback=True)
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())
    with TestClient(sync.create_app(), client=("127.0.0.1", 50000)) as client:
        response = client.post(
            "/sync/auth/passphrase",
            json={"passphrase": "attacker-password"},
            headers={
                "origin": "http://attacker.example",
                "host": "attacker.example",
            },
        )
        assert response.status_code == 403
        assert auth.verify_passphrase("TEST-PASS-CODE") is True


def test_revoked_api_token_is_disconnected_before_another_command(tmp_path):
    auth = LanPassphraseAuth(
        "long-enough-password",
        store=SQLiteAuthStore(tmp_path / "auth.db"),
        trust_loopback=False,
    )
    credential = auth.create_api_token("controller", capabilities={"control"})
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())

    @sync.command
    def act():
        return "acted"

    with TestClient(sync.create_app()) as client:
        headers = {"authorization": f"Bearer {credential.token}"}
        with client.websocket_connect("/sync/ws", headers=headers) as websocket:
            websocket.receive_json()
            assert auth.revoke_api_token(credential.id) is True
            websocket.send_json(
                {
                    "type": "command",
                    "command": "act",
                    "params": {},
                    "requestId": "revoked-1",
                }
            )
            with pytest.raises(WebSocketDisconnect) as exc_info:
                websocket.receive_json()
            assert exc_info.value.code == 4401


def test_rotating_passphrase_revokes_remembered_sessions(tmp_path):
    path = tmp_path / "auth.db"
    auth = LanPassphraseAuth(
        "long-enough-password",
        store=SQLiteAuthStore(path),
        trust_loopback=False,
    )
    sync = LabSync(auth=auth)
    sync.bind_state(ProtectedState())
    invite = auth.create_invite()
    with TestClient(sync.create_app()) as client:
        client.post(
            "/sync/auth/login",
            json={"passphrase": "long-enough-password", "remember": True},
        )
        assert client.get("/sync/state").status_code == 200
        changed = client.post(
            "/sync/auth/passphrase",
            json={"passphrase": "a-new-long-password", "revokeSessions": True},
        )
        assert changed.status_code == 200
        assert client.get("/sync/state").status_code == 401
        assert auth.verify_passphrase("long-enough-password") is False
        assert auth.verify_passphrase("a-new-long-password") is True
        assert auth.invite_status(invite.id) == "revoked"
