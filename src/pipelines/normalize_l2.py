import json
import time
from pathlib import Path
from typing import Optional

import orjson

from src.core.schemas.l2_delta import (
    is_depth_delta,
    normalize_delta,
)
from src.validation.audit import Audit
from src.validation.continuity import continuity_ok


def list_raw_files(raw_dir: Path):
    return sorted(p for p in raw_dir.glob("deltas_utcmin_*.jsonl") if p.is_file())


def process_raw_dir(raw_dir: Path) -> None:
    raw_dir = raw_dir.resolve()
    base = raw_dir.parent

    norm_dir = base / "normalized"
    audit_dir = base / "audit"

    norm_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    for raw_file in list_raw_files(raw_dir):
        stem = raw_file.stem
        norm_file = norm_dir / f"{stem}.fp.jsonl"
        audit_file = audit_dir / f"{stem}.audit.json"

        if norm_file.exists() and audit_file.exists():
            continue

        audit = Audit()
        prev_u: Optional[int] = None

        with raw_file.open("rb") as fin, norm_file.open("wb") as fnorm:
            for line in fin:
                audit.raw_lines += 1

                try:
                    raw = orjson.loads(line)
                except Exception:
                    audit.bad_json += 1
                    continue

                if not is_depth_delta(raw):
                    audit.skipped_schema += 1
                    continue

                try:
                    d = normalize_delta(raw)
                except Exception:
                    audit.normalize_errors += 1
                    continue

                audit.kept_lines += 1

                U, u = d["U"], d["u"]
                if not continuity_ok(prev_u, U, u):
                    audit.continuity_ok = False
                    audit.gaps += 1
                    if audit.first_gap is None:
                        audit.first_gap = {
                            "prev_u": prev_u,
                            "U": U,
                            "u": u,
                            "line": audit.raw_lines,
                        }
                    prev_u = None
                else:
                    prev_u = u

                fnorm.write(orjson.dumps(d) + b"\n")

        with audit_file.open("w", encoding="utf-8") as fa:
            json.dump(
                {
                    "raw_file": str(raw_file),
                    "normalized_file": str(norm_file),
                    "created_at_unix": int(time.time()),
                    "stats": audit.__dict__,
                },
                fa,
                indent=2,
            )
