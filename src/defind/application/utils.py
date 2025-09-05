from datetime import datetime, timezone


def _fp(topic0s: list[str]) -> str:
    import hashlib as h
    base = ",".join(sorted([t.lower() for t in topic0s]))
    return h.sha1(base.encode()).hexdigest()[:10]


def _now_ts_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y_%m_%d_%H:%M:%S")