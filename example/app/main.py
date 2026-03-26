import random
import time
from pathlib import Path

from fastapi.staticfiles import StaticFiles
from lab_sync import LabSync
from pydantic import BaseModel

sync = LabSync()

# Rolling 600-point history (30 s at 20 Hz)
sensor_history = sync.stream("sensor_history", mode="append", capacity=100)


@sync.state
class SensorState(BaseModel):
    active: bool = True
    value: float = 0.0
    last_updated: float = 0.0


@sync.command
def toggle():
    sync.state.active = not sync.state.active


@sync.updater(interval=0.016)  # 20 Hz — drives the graph stream
async def fast_tick():
    if sync.get("active"):
        await sensor_history.append(round(random.gauss(22.0, 2.0), 2))


@sync.updater(interval=0.5)  # 2 Hz — updates the displayed number
def slow_tick():
    if sync.get("active"):
        sync.state.value = round(random.gauss(22.0, 2.0), 2)
        sync.state.last_updated = time.time()


app = sync.create_app()

# Serve BokehJS static files from the sibling server-lab-json-patch project.
_bokeh_assets = (
    Path(__file__).parent.parent.parent.parent
    / "server-lab-json-patch/app/web_dist/assets"
)
if _bokeh_assets.exists():
    app.mount("/assets", StaticFiles(directory=_bokeh_assets), name="assets")
