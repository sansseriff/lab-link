"""
Smoke test / minimal usage example for lab-sync.

Run:
    uv run uvicorn example:app

Then:
    curl http://localhost:8000/sync/state
    wscat -c ws://localhost:8000/sync/ws
    # send: {"type":"command","command":"set_x","params":{"value":5},"requestId":"1"}
    # receive: command_ack + patch
"""
from lab_sync import LabSync
from pydantic import BaseModel

sync = LabSync()


@sync.state
class S(BaseModel):
    x: float = 0.0


@sync.command
def set_x(value: float):
    sync.state.x = value


app = sync.create_app()
