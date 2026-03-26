import asyncio

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import BaseModel

from lab_sync import LabSync


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
