from dataclasses import dataclass
from typing import Optional


@dataclass
class Audit:
    raw_lines: int = 0
    kept_lines: int = 0
    bad_json: int = 0
    skipped_schema: int = 0
    normalize_errors: int = 0
    continuity_ok: bool = True
    gaps: int = 0
    first_gap: Optional[dict] = None
