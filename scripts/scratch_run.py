# scratch_run.py
import asyncio
from pathlib import Path

from src.ingestion.adapters.binance.record_binance_depth import record_binance_depth

async def main():
    await record_binance_depth(
        symbol="BTCUSDT",
        out_dir=Path("data/binance/BTCUSDT/raw"),
        interval_ms=100,
    )

asyncio.run(main())
