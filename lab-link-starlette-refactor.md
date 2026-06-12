# Refactoring lab-link from FastAPI to Starlette

## Why

lab-link uses no FastAPI-distinctive features — no dependency injection, no
request/body validation on routes, no OpenAPI (dbay explicitly disables the
docs endpoints). The `WebSocket` and `WebSocketDisconnect` classes it imports
from `fastapi` are Starlette classes that FastAPI merely re-exports. The whole
sync engine (StateStore, ConnectionManager, command dispatch, stream buffers)
only calls the Starlette WebSocket API: `accept()`, `receive_json()`,
`send_json()`, `send_bytes()`, `close()`.

Measured in the dbay backend venv (Python 3.13, Apple Silicon):

| | import time | on disk |
|---|---|---|
| starlette | 79 ms | 644 KB |
| fastapi (incl. starlette + pydantic) | 185 ms | +1.3 MB |

Since pydantic and starlette load either way, dropping FastAPI saves roughly
60 ms of startup and 1.3 MB in the PyInstaller bundle. Modest — the real
motivation is honesty about the dependency surface and decoupling from
FastAPI's release cadence.

## Changes in lab-link

1. **Swap the websocket imports** (zero behavioral change — same classes):

   ```python
   # core.py, connection_manager.py
   from starlette.websockets import WebSocket, WebSocketDisconnect
   ```

2. **Make `_handle_ws` public.** Rename to `handle_ws(websocket)`. Consumers
   (dbay already does this) attach it to whatever route they like. This is the
   primary integration point; the router becomes a convenience.

3. **Replace `APIRouter` with a Starlette router.** The current `router`
   property exposes `GET {prefix}/state` and `WS {prefix}/ws`:

   ```python
   from starlette.routing import Route, WebSocketRoute, Router

   @property
   def routes(self) -> list:
       async def get_state(request):
           return JSONResponse(self._store.snapshot() if self._store else {})
       return [
           Route(f"{self._prefix}/state", get_state),
           WebSocketRoute(f"{self._prefix}/ws", self.handle_ws),
       ]
   ```

4. **`create_app()` builds a `Starlette` app** instead of `FastAPI`, wiring
   `lifespan=` and `routes=`. The `lifespan()` context manager itself is
   framework-agnostic already — only its type hint mentions `FastAPI`; change
   it to `app: Any | None = None`.

5. **pyproject**: replace `fastapi>=0.115` with `starlette>=0.40`. Optionally
   add an extra `fastapi = ["fastapi>=0.115"]` if you want to keep shipping a
   `create_fastapi_router()` helper, but see below — it isn't really needed.

6. *(Unrelated hardening found while wiring dbay persistence)*: in
   `LabSync.lifespan`, `PersistenceManager.initialize()` is not wrapped in
   try/except — a corrupt SQLite file crashes server startup. Only the
   `replace_state(saved)` call is guarded. Worth wrapping both.

## FastAPI compatibility — yes, it stays easy

FastAPI **is** a Starlette subclass; every FastAPI app is a Starlette app.
So an app that uses Starlette-based lab-link can additionally install FastAPI
and get all of its features for its own endpoints:

```python
from fastapi import FastAPI, WebSocket

app = FastAPI(lifespan=lifespan)          # DI, OpenAPI, validation — all available

@app.websocket("/sync/ws")
async def sync_ws(ws: WebSocket):          # fastapi's WebSocket *is* starlette's
    await sync.handle_ws(ws)

@app.get("/sync/state")
async def state():
    return sync.get("")
```

That is exactly the wiring dbay uses today (`backend/sync.py`), so dbay would
not change at all — or it could drop FastAPI too, since its `main.py` only
needs `StaticFiles`, `CORSMiddleware`, one `FileResponse` for the Svelte SPA's
`index.html`, and the websocket route, all of which Starlette provides
directly.

The one-way door to avoid: don't make lab-link's public API *require* FastAPI
types (e.g. don't return an `APIRouter`). Starlette-level handlers compose
upward into FastAPI; FastAPI objects don't compose downward into plain
Starlette/uvicorn deployments.

## What not to do

Don't replace the server with a sync WSGI framework (bottle, flask). The sync
engine is asyncio throughout (patch queue, broadcast tasks, `@sync.updater`
loops), and WSGI frameworks can't host websockets without gevent-style hacks —
you'd end up running two servers. Starlette + uvicorn is already the minimal
async stack that serves both the SPA and the websocket.
