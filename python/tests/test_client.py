from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
import pytest_asyncio
from websockets.asyncio.server import serve

from lab_link import AsyncLabLinkClient, LabLinkClient, SyncCommandError


@pytest_asyncio.fixture
async def protocol_server():
    received: list[dict[str, Any]] = []

    async def handler(websocket):
        await websocket.send(
            json.dumps(
                {
                    "type": "snapshot",
                    "data": {"x": 0.0, "label": "hello"},
                    "version": 0,
                }
            )
        )

        async for raw in websocket:
            msg = json.loads(raw)
            received.append(msg)
            command = msg["command"]
            request_id = msg["requestId"]

            if command == "set_x":
                value = msg["params"]["value"]
                await websocket.send(
                    json.dumps(
                        {
                            "type": "patch",
                            "patch": [{"op": "replace", "path": "/x", "value": value}],
                            "version": 1,
                            "originClientId": "client-1",
                            "requestId": request_id,
                            "command": command,
                        }
                    )
                )
                await websocket.send(
                    json.dumps(
                        {
                            "type": "command_ack",
                            "command": command,
                            "requestId": request_id,
                            "version": 1,
                            "result": {"ok": True},
                        }
                    )
                )
            elif command == "fail":
                await websocket.send(
                    json.dumps(
                        {
                            "type": "command_error",
                            "command": command,
                            "requestId": request_id,
                            "code": "hardware_timeout",
                            "message": "Timed out.",
                            "detail": "timeout after 5 s",
                            "severity": "error",
                            "display": "banner",
                            "recoverable": True,
                            "path": "/x",
                            "originClientId": "client-1",
                            "version": 0,
                        }
                    )
                )

    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}", received
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_async_client_connects_applies_patch_before_ack(protocol_server):
    url, received = protocol_server
    patches = []

    async with AsyncLabLinkClient(url) as client:
        client.on_patch(patches.append)

        snapshot = client.snapshot()
        assert snapshot == {"x": 0.0, "label": "hello"}
        snapshot["x"] = 99.0
        assert client.snapshot() == {"x": 0.0, "label": "hello"}

        ack = await client.send_command(
            "set_x",
            {"value": 7.5},
            request_id="req-1",
        )

        assert ack.command == "set_x"
        assert ack.request_id == "req-1"
        assert ack.version == 1
        assert ack.result == {"ok": True}
        assert client.snapshot() == {"x": 7.5, "label": "hello"}
        assert client.version == 1

    assert received == [
        {
            "type": "command",
            "command": "set_x",
            "params": {"value": 7.5},
            "requestId": "req-1",
        }
    ]
    assert patches[0].request_id == "req-1"
    assert patches[0].origin_client_id == "client-1"
    assert patches[0].command == "set_x"


@pytest.mark.asyncio
async def test_async_client_raises_structured_command_error(protocol_server):
    url, _ = protocol_server
    errors = []

    async with AsyncLabLinkClient(url) as client:
        client.on_command_error(errors.append)

        with pytest.raises(SyncCommandError) as exc_info:
            await client.send_command("fail", {}, request_id="req-error")

    error = exc_info.value
    assert error.command == "fail"
    assert error.request_id == "req-error"
    assert error.code == "hardware_timeout"
    assert error.message == "Timed out."
    assert error.detail == "timeout after 5 s"
    assert error.display == "banner"
    assert error.path == "/x"
    assert errors == [error]


@pytest.mark.asyncio
async def test_async_client_command_timeout(protocol_server):
    url, _ = protocol_server

    async with AsyncLabLinkClient(url) as client:
        with pytest.raises(SyncCommandError) as exc_info:
            await client.send_command(
                "hang", {}, request_id="req-timeout", timeout=0.01
            )

    error = exc_info.value
    assert error.command == "hang"
    assert error.request_id == "req-timeout"
    assert error.code == "command_timeout"
    assert client.last_errors() == [error]


@pytest.mark.asyncio
async def test_sync_client_wrapper(protocol_server):
    url, _ = protocol_server

    def run_client():
        with LabLinkClient(url) as client:
            ack = client.send_command(
                "set_x",
                {"value": 3.25},
                request_id="req-sync",
            )
            return client.snapshot(), ack

    snapshot, ack = await asyncio.to_thread(run_client)

    assert snapshot == {"x": 3.25, "label": "hello"}
    assert ack.request_id == "req-sync"
    assert ack.version == 1


def test_clients_add_bearer_header_for_api_token():
    async_client = AsyncLabLinkClient(
        "ws://instrument.test/sync/ws", api_token="ll_secret"
    )
    headers = async_client.websocket_kwargs.get(
        "additional_headers", async_client.websocket_kwargs.get("extra_headers")
    )
    assert headers == {"Authorization": "Bearer ll_secret"}

    sync_client = LabLinkClient("ws://instrument.test/sync/ws", api_token="ll_secret")
    headers = sync_client._async_client.websocket_kwargs.get(
        "additional_headers",
        sync_client._async_client.websocket_kwargs.get("extra_headers"),
    )
    assert headers == {"Authorization": "Bearer ll_secret"}
