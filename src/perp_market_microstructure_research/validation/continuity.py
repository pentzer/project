from typing import Optional


def continuity_ok(prev_u: Optional[int], U: int, u: int) -> bool:
    if prev_u is None:
        return True
    return U <= prev_u + 1 <= u
