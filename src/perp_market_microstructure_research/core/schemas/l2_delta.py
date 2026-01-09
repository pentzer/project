from perp_market_microstructure_research.core.fixed_point import normalize_level


def is_depth_delta(raw: dict) -> bool:
    required = (
        "exchange",
        "symbol",
        "conn_id",
        "recv_ts_ns",
        "event_ts_ms",
        "U",
        "u",
        "b",
        "a",
    )
    for k in required:
        if k not in raw:
            return False
    return True


def normalize_delta(raw: dict) -> dict:
    return {
        "exchange": raw["exchange"],
        "symbol": raw["symbol"],
        "conn_id": raw["conn_id"],
        "recv_ts_ns": raw["recv_ts_ns"],
        "event_ts_ns": raw["event_ts_ms"] * 1_000_000,
        "U": raw["U"],
        "u": raw["u"],
        "b": [normalize_level(l) for l in raw["b"]],
        "a": [normalize_level(l) for l in raw["a"]],
    }
