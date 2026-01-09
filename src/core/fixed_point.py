from decimal import Decimal, getcontext
from typing import Tuple

# =========================
# CONFIG (SCHEMA-LEVEL)
# =========================

PRICE_SCALE = 10**8
QTY_SCALE = 10**8

getcontext().prec = 50


# =========================
# FIXED-POINT HELPERS
# =========================

def to_fp(x: str, scale: int) -> int:
    return int((Decimal(x) * scale).to_integral_exact())


def normalize_level(level: list) -> Tuple[int, int]:
    return to_fp(level[0], PRICE_SCALE), to_fp(level[1], QTY_SCALE)
