from __future__ import annotations

import struct
from collections import deque
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from .connection_manager import ConnectionManager


class AppendBuffer:
    """Ring buffer for time-series streams (sensor history, logs)."""

    def __init__(self, id: str, capacity: int, conn_manager: "ConnectionManager") -> None:
        self.id = id
        self._capacity = capacity
        self._conn_manager = conn_manager
        self._buffer: deque[Any] = deque(maxlen=capacity)
        self._seq: int = 0

    async def append(self, point: Any) -> None:
        self._buffer.append(point)
        self._seq += 1
        await self._conn_manager.broadcast_json(
            {"type": "stream_append", "id": self.id, "data": [point], "seq": self._seq}
        )

    async def extend(self, points: list[Any]) -> None:
        self._buffer.extend(points)
        self._seq += 1
        await self._conn_manager.broadcast_json(
            {"type": "stream_append", "id": self.id, "data": points, "seq": self._seq}
        )

    def snapshot_message(self) -> dict[str, Any]:
        return {
            "type": "stream_snapshot",
            "id": self.id,
            "data": list(self._buffer),
            "seq": self._seq,
        }


class ReplaceBuffer:
    """Full-replace buffer for float arrays (FFT, KDE, waveform)."""

    def __init__(
        self,
        id: str,
        capacity: int,
        dtype: str,
        conn_manager: "ConnectionManager",
    ) -> None:
        self.id = id
        self._capacity = capacity
        self._dtype = dtype
        self._conn_manager = conn_manager
        self._data: list[float] = []
        self._seq: int = 0

    async def replace(self, data: "list[float] | Any") -> None:
        try:
            import numpy as np
            if isinstance(data, np.ndarray):
                data = data.tolist()
        except ImportError:
            pass
        self._data = list(data)
        self._seq += 1

        if self._dtype in ("float32", "float64"):
            frame = self._encode_binary(self._data)
            await self._conn_manager.broadcast_binary(frame)
        else:
            await self._conn_manager.broadcast_json(
                {"type": "stream_replace", "id": self.id, "data": self._data, "seq": self._seq}
            )

    def _encode_binary(self, data: list[float]) -> bytes:
        id_bytes = self.id.encode("utf-8")
        header = struct.pack("<BH", 0x01, len(id_bytes)) + id_bytes + struct.pack("<I", self._seq)
        fmt = "<" + ("f" if self._dtype == "float32" else "d") * len(data)
        payload = struct.pack(fmt, *data)
        return header + payload

    def snapshot_message(self) -> dict[str, Any]:
        return {
            "type": "stream_snapshot",
            "id": self.id,
            "data": self._data,
            "seq": self._seq,
        }


class DeltaBuffer:
    """Sparse-delta buffer for integer-count histograms."""

    def __init__(self, id: str, num_bins: int, conn_manager: "ConnectionManager") -> None:
        self.id = id
        self._num_bins = num_bins
        self._conn_manager = conn_manager
        self._bins: list[int] = [0] * num_bins
        self._seq: int = 0

    async def apply_delta(self, deltas: dict[int, int]) -> None:
        for idx, delta in deltas.items():
            self._bins[idx] += delta
        self._seq += 1
        sparse = [[idx, delta] for idx, delta in deltas.items() if delta != 0]
        await self._conn_manager.broadcast_json(
            {"type": "stream_delta", "id": self.id, "deltas": sparse, "seq": self._seq}
        )

    async def replace(self, bins: list[int]) -> None:
        self._bins = list(bins)
        self._seq += 1
        await self._conn_manager.broadcast_json(self.snapshot_message())

    def snapshot_message(self) -> dict[str, Any]:
        return {
            "type": "stream_snapshot",
            "id": self.id,
            "data": list(self._bins),
            "seq": self._seq,
        }


class StreamRef:
    """
    Lazy reference returned by ``sync.stream()``.

    Holds the stream spec (id, mode, capacity, dtype) until the app lifespan starts
    and materialises the real buffer. After that all method calls are forwarded.

    This lets user code store the return value of ``sync.stream()`` at module level
    and call ``await buf.append(...)`` inside updaters without worrying about
    whether the lifespan has started yet.
    """

    def __init__(
        self,
        id: str,
        mode: Literal["append", "replace", "int_delta"],
        capacity: int,
        dtype: Literal["float32", "float64", "json"],
    ) -> None:
        self.id = id
        self.mode = mode
        self.capacity = capacity
        self.dtype = dtype
        self._buffer: AppendBuffer | ReplaceBuffer | DeltaBuffer | None = None

    def materialize(self, buffer: AppendBuffer | ReplaceBuffer | DeltaBuffer) -> None:
        self._buffer = buffer

    def _require(self) -> AppendBuffer | ReplaceBuffer | DeltaBuffer:
        if self._buffer is None:
            raise RuntimeError(
                f"Stream {self.id!r} is not yet active — "
                "ensure it is accessed after the app lifespan has started."
            )
        return self._buffer

    # ── AppendBuffer interface ────────────────────────────────────────────────

    async def append(self, point: Any) -> None:
        buf = self._require()
        assert isinstance(buf, AppendBuffer)
        await buf.append(point)

    async def extend(self, points: list[Any]) -> None:
        buf = self._require()
        assert isinstance(buf, AppendBuffer)
        await buf.extend(points)

    # ── ReplaceBuffer interface ───────────────────────────────────────────────

    async def replace(self, data: Any) -> None:
        buf = self._require()
        assert isinstance(buf, ReplaceBuffer)
        await buf.replace(data)

    # ── DeltaBuffer interface ─────────────────────────────────────────────────

    async def apply_delta(self, deltas: dict[int, int]) -> None:
        buf = self._require()
        assert isinstance(buf, DeltaBuffer)
        await buf.apply_delta(deltas)

    def snapshot_message(self) -> dict[str, Any]:
        return self._require().snapshot_message()
