import asyncio

import pytest
from pydantic import BaseModel

from lab_link.proxy import StateProxy
from lab_link.state_store import StateStore


class S(BaseModel):
    x: float = 0.0
    pump: dict = {"speed": 0, "running": False}


def _make_proxy():
    store = StateStore(S, {"x": 0.0, "pump": {"speed": 0, "running": False}})
    queue = asyncio.Queue()
    proxy = StateProxy(store, queue)
    return proxy, store, queue


def test_scalar_read():
    proxy, store, _ = _make_proxy()
    assert proxy.x == 0.0


def test_scalar_write_enqueues():
    proxy, store, queue = _make_proxy()
    proxy.x = 5.0
    path, value = queue.get_nowait()
    assert path == "/x"
    assert value == 5.0


def test_nested_write_enqueues():
    proxy, store, queue = _make_proxy()
    proxy.pump.speed = 1500
    path, value = queue.get_nowait()
    assert path == "/pump/speed"
    assert value == 1500


def test_nested_dict_returns_nested_proxy():
    from lab_link.proxy import NestedProxy
    proxy, _, _ = _make_proxy()
    result = proxy.pump
    assert isinstance(result, NestedProxy)


def test_rebind_queue():
    proxy, store, old_queue = _make_proxy()
    new_queue = asyncio.Queue()
    proxy._rebind_queue(new_queue)
    proxy.x = 42.0
    assert old_queue.empty()
    path, value = new_queue.get_nowait()
    assert path == "/x"
    assert value == 42.0
