import random
import time
from pathlib import Path

from starlette.staticfiles import StaticFiles
from lab_link import LabSync, ReactiveModel

sync = LabSync()

# Rolling 600-point history (30 s at 20 Hz)
sensor_history = sync.stream("sensor_history", mode="append", capacity=100)


class SensorState(ReactiveModel):
    active: bool = True
    value: float = 0.0
    last_updated: float = 0.0


state = sync.bind_state(SensorState())


@sync.command
def toggle():
    state.active = not state.active


@sync.updater(interval=0.016)  # 20 Hz — drives the graph stream
async def fast_tick():
    if state.active:
        await sensor_history.append(round(random.gauss(22.0, 2.0), 2))


@sync.updater(interval=0.5)  # 2 Hz — updates the displayed number
def slow_tick():
    if state.active:
        state.value = round(random.gauss(22.0, 2.0), 2)
        state.last_updated = time.time()


app = sync.create_app()

# Serve BokehJS static files from the sibling server-lab-json-patch project.
_bokeh_assets = (
    Path(__file__).parent.parent.parent.parent
    / "server-lab-json-patch/app/web_dist/assets"
)
if _bokeh_assets.exists():
    app.mount("/assets", StaticFiles(directory=_bokeh_assets), name="assets")
