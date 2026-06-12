"""
Smoke test / minimal usage example for lab-link.

Run:
    uv run uvicorn example:app

Then:
    curl http://localhost:8000/sync/state
    wscat -c ws://localhost:8000/sync/ws
    # send: {"type":"command","command":"set_x","params":{"value":5},"requestId":"1"}
    # receive: patch + command_ack
"""
from lab_link import LabSync, ReactiveModel

sync = LabSync()


class S(ReactiveModel):
    x: float = 0.0


state = sync.bind_state(S())


@sync.command
def set_x(value: float):
    state.x = value


app = sync.create_app()
