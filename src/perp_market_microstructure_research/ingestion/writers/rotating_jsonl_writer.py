import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# =========================
# CONFIG
# =========================

BATCH_SIZE = 2000
FLUSH_INTERVAL_SEC = 0.5


# =========================
# DATA MODEL
# =========================

@dataclass
class WriteItem:
    bucket: int
    line: bytes


# =========================
# WRITER
# =========================

class RotatingJSONLWriter:
    """
    Append-only JSONL writer with time-bucketed rotation.

    - One file per logical time bucket
    - Writes to `.tmp` and atomically renames on finalize
    - Buffered writes for throughput
    """

    def __init__(self, out_dir: Path, *, filename_fn):
        self.out_dir = out_dir
        self.filename_fn = filename_fn

        self.current_bucket: Optional[int] = None
        self.fp = None
        self.tmp_path: Optional[Path] = None
        self.final_path: Optional[Path] = None

        self.buffer: list[bytes] = []
        self.last_flush: float = time.time()

    def _open_for_bucket(self, bucket: int) -> None:
        """
        Open a new file for the given bucket.

        Assumes that any previous bucket has already been finalized.
        """
        self.out_dir.mkdir(parents=True, exist_ok=True)

        name = self.filename_fn(bucket)
        self.final_path = self.out_dir / name
        self.tmp_path = self.out_dir / f"{name}.tmp"

        # append mode allows restart/reconnect safety
        self.fp = self.tmp_path.open("ab", buffering=1024 * 1024)
        self.current_bucket = bucket
        self.last_flush = time.time()

    def _close_and_finalize(self) -> None:
        """
        Flush buffer, close file, and atomically rename tmp → final.
        """
        if self.fp is None:
            return

        if self.buffer:
            self.fp.write(b"".join(self.buffer))
            self.buffer.clear()

        self.fp.flush()
        # Optional durability hook (leave disabled unless needed)
        # os.fsync(self.fp.fileno())

        self.fp.close()
        self.tmp_path.replace(self.final_path)

        self.fp = None
        self.current_bucket = None
        self.tmp_path = None
        self.final_path = None

    def write(self, item: WriteItem) -> None:
        """
        Write a single record.

        Buckets are assumed to be monotonic or discontinuous jumps;
        no attempt is made to backfill missing buckets.
        """
        if self.current_bucket is None or item.bucket != self.current_bucket:
            self._close_and_finalize()
            self._open_for_bucket(item.bucket)

        self.buffer.append(item.line)

        now = time.time()
        if (
            len(self.buffer) >= BATCH_SIZE
            or (now - self.last_flush) >= FLUSH_INTERVAL_SEC
        ):
            self.fp.write(b"".join(self.buffer))
            self.fp.flush()
            self.buffer.clear()
            self.last_flush = now

    def close(self) -> None:
        self._close_and_finalize()


# =========================
# ASYNC LOOP
# =========================

async def writer_loop(
    *,
    out_dir: Path,
    filename_fn,
    queue: asyncio.Queue,
) -> None:
    """
    Async consumer loop for WriteItem events.

    Terminates cleanly on `None` sentinel.
    """
    writer = RotatingJSONLWriter(out_dir, filename_fn=filename_fn)
    try:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            writer.write(item)
            queue.task_done()
    finally:
        writer.close()
