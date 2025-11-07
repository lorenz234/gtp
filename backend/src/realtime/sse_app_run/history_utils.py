from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional


def iso_from_ms(ts_ms: int) -> str:
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def encode_history_entry(value: float, timestamp_ms: int, is_ath: bool) -> str:
    entry = {"v": round(value, 3), "ms": timestamp_ms}
    if is_ath:
        entry["a"] = 1
    return json.dumps(entry, separators=(",", ":"))


def decode_history_entry(raw_entry: Any) -> Dict[str, Any]:
    if raw_entry is None:
        return {}
    if isinstance(raw_entry, bytes):
        raw_entry = raw_entry.decode("utf-8")
    try:
        data = json.loads(raw_entry)
    except (json.JSONDecodeError, TypeError):
        return {}

    if "v" in data:
        ts_ms = int(data.get("ms", 0))
        return {
            "tps": float(data.get("v", 0.0)),
            "timestamp_ms": ts_ms,
            "timestamp": iso_from_ms(ts_ms),
            "is_ath": bool(data.get("a", 0)),
        }

    ts_ms = int(data.get("timestamp_ms", data.get("timestampMs", 0)) or 0)
    timestamp = data.get("timestamp") or iso_from_ms(ts_ms)
    return {
        "tps": float(data.get("tps", data.get("value", 0.0))),
        "timestamp_ms": ts_ms,
        "timestamp": timestamp,
        "is_ath": bool(data.get("is_ath", data.get("isATH", False))),
    }


class HistoryCompressor:
    """Drop interior monotonic points so Redis histories stay compact."""

    def __init__(self, epsilon: float = 1e-3) -> None:
        self.epsilon = epsilon
        self._cache: Dict[str, Deque[Dict[str, Any]]] = {}

    def _trend_direction(self, first: float, second: float) -> int:
        delta = second - first
        if abs(delta) <= self.epsilon:
            return 0
        return 1 if delta > 0 else -1

    def register(self, key: str, value: float, timestamp_ms: int, payload: str) -> Optional[str]:
        cache = self._cache.setdefault(key, deque(maxlen=3))
        redundant_payload = None
        if len(cache) >= 2:
            prev = cache[-1]
            prev_prev = cache[-2]
            prev_dir = self._trend_direction(prev_prev["v"], prev["v"])
            new_dir = self._trend_direction(prev["v"], value)
            if prev_dir != 0 and prev_dir == new_dir:
                redundant_payload = cache.pop()["payload"]
        cache.append({"v": value, "ms": timestamp_ms, "payload": payload})
        return redundant_payload


__all__ = ["HistoryCompressor", "encode_history_entry", "decode_history_entry", "iso_from_ms"]
