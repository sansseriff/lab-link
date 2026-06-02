import asyncio

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import BaseModel

from lab_link import CommandContext, CommandError, LabSync


class AppState(BaseModel):
    x: float = 0.0
    label: str = "hello"


@pytest.fixture
def sync_app():
    sync = LabSync()

    @sync.state
    class S(AppState):
        pass

    @sync.command
    def set_x(value: float):
        sync.state.x = value

    app = sync.create_app()
    return app, sync


def test_state_endpoint(sync_app):
    app, sync = sync_app
    with TestClient(app) as client:
        resp = client.get("/sync/state")
        assert resp.status_code == 200
        data = resp.json()
        assert data["x"] == 0.0
        assert data["label"] == "hello"


def test_websocket_snapshot(sync_app):
    app, sync = sync_app
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            msg = ws.receive_json()
            assert msg["type"] == "snapshot"
            assert msg["data"]["x"] == 0.0
            assert "version" in msg


def test_websocket_command_ack(sync_app):
    app, sync = sync_app
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            # consume snapshot
            ws.receive_json()
            ws.send_json({
                "type": "command",
                "command": "set_x",
                "params": {"value": 7.0},
                "requestId": "req-1",
            })
            # may receive patch before ack
            messages = []
            for _ in range(3):
                try:
                    msg = ws.receive_json()
                    messages.append(msg)
                    if msg["type"] == "command_ack":
                        break
                except Exception:
                    break
            types = [m["type"] for m in messages]
            assert "command_ack" in types
            patch_msg = next(m for m in messages if m["type"] == "patch")
            assert patch_msg["requestId"] == "req-1"
            assert patch_msg["command"] == "set_x"
            assert "originClientId" in patch_msg


def test_websocket_unknown_command(sync_app):
    app, sync = sync_app
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            ws.receive_json()  # snapshot
            ws.send_json({
                "type": "command",
                "command": "nonexistent",
                "params": {},
                "requestId": "req-2",
            })
            msg = ws.receive_json()
            assert msg["type"] == "command_error"
            assert msg["requestId"] == "req-2"
            assert msg["code"] == "unknown_command"


def test_register_state_runtime_initial_instance():
    sync = LabSync()
    sync.register_state(AppState, initial=AppState(x=3.5, label="runtime"))
    app = sync.create_app()
    with TestClient(app) as client:
        resp = client.get("/sync/state")
        assert resp.json() == {"x": 3.5, "label": "runtime"}


def test_command_context_result_and_patch_metadata():
    sync = LabSync()
    sync.register_state(AppState, initial={"x": 0.0, "label": "hello"})

    @sync.command
    def set_x(ctx: CommandContext, value: float):
        sync.set("/x", value)
        return {"path": "/x", "value": value, "client_id": ctx.client_id}

    app = sync.create_app()
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            ws.receive_json()
            ws.send_json({
                "type": "command",
                "command": "set_x",
                "params": {"value": 2.5},
                "requestId": "req-meta",
            })
            patch_msg = ws.receive_json()
            ack_msg = ws.receive_json()

            assert patch_msg["type"] == "patch"
            assert patch_msg["requestId"] == "req-meta"
            assert patch_msg["command"] == "set_x"
            assert patch_msg["originClientId"] == ack_msg["result"]["client_id"]
            assert patch_msg["patch"] == [{"op": "replace", "path": "/x", "value": 2.5}]

            assert ack_msg["type"] == "command_ack"
            assert ack_msg["requestId"] == "req-meta"
            assert ack_msg["version"] == patch_msg["version"]
            assert ack_msg["result"]["path"] == "/x"


def test_transaction_emits_one_patch_batch_and_one_version():
    sync = LabSync()
    sync.register_state(AppState, initial={"x": 0.0, "label": "hello"})

    @sync.command
    def update_many():
        with sync.transaction():
            pass
        with sync.transaction() as tx:
            tx.set("/x", 4.0)
            tx.set("/label", "updated")

    app = sync.create_app()
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            ws.receive_json()
            ws.send_json({
                "type": "command",
                "command": "update_many",
                "params": {},
                "requestId": "req-tx",
            })
            patch_msg = ws.receive_json()
            ack_msg = ws.receive_json()
            assert patch_msg["type"] == "patch"
            assert patch_msg["version"] == 1
            assert len(patch_msg["patch"]) == 2
            assert ack_msg["version"] == 1


def test_structured_command_error():
    sync = LabSync()
    sync.register_state(AppState, initial={"x": 0.0, "label": "hello"})

    @sync.command
    def fail_hardware():
        raise CommandError(
            code="hardware_timeout",
            message="The voltage source did not respond before the timeout.",
            detail="UDP timeout after 5.0 s",
            display="banner",
            path="/x",
            recoverable=True,
        )

    app = sync.create_app()
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            ws.receive_json()
            ws.send_json({
                "type": "command",
                "command": "fail_hardware",
                "params": {},
                "requestId": "req-error",
            })
            msg = ws.receive_json()
            assert msg["type"] == "command_error"
            assert msg["code"] == "hardware_timeout"
            assert msg["message"] == "The voltage source did not respond before the timeout."
            assert msg["detail"] == "UDP timeout after 5.0 s"
            assert msg["display"] == "banner"
            assert msg["path"] == "/x"
            assert msg["version"] == 0


def test_command_error_constructs_as_exception():
    error = CommandError(code="hardware_timeout", message="Timed out.")

    assert isinstance(error, Exception)
    assert error.args == ("Timed out.",)
