Canonical Target Structure (Authoritative)

This structure describes how the system is supposed to work.

src/
├── core/
│   ├── fixed_point.py
│   ├── prices.py
│   ├── schemas/
│   │   └── l2_delta.py
│   └── orderbook/
│       └── top.py
│
├── validation/
│   ├── continuity.py
│   └── audit.py
│
├── ingestion/
│   ├── adapters/
│   │   └── binance/
│   │       └── depth_recorder.py        
│   └── writers/
│       └── rotating_jsonl_writer.py     
│
├── pipelines/
│   ├── record_live.py                   
│   └── normalize_l2.py                  
│
scripts/
├── run_recorder.py
└── run_normalize.py


Nothing here is arbitrary. Each directory corresponds to a causality layer.



Responsibility Contracts (this is the key)

Below is the non-negotiable contract for each layer.

1️⃣ core/ — Pure logic (no I/O, no paths)

Rule:

Code here must be deterministic, importable, and testable in isolation.

core/fixed_point.py

Owns:

PRICE_SCALE, QTY_SCALE

to_fp

normalize_level

Never:

reads files

writes files

knows what an exchange is

core/schemas/l2_delta.py

Owns:

is_depth_delta

normalize_delta

normalized schema definition

This is the contract between ingestion and everything downstream.

core/orderbook/top.py

Owns:

TopBook

apply

best

No normalization. No audit. No prices beyond what it computes.

core/prices.py

Owns:

mid_fp

micro_fp

Later this file will grow (VWAP, imbalance, slope, etc.)
This is intentional.

2️⃣ validation/ — Orthogonal correctness checks

Rule:

Validation never mutates data. It only observes.

validation/continuity.py

Owns:

continuity_ok

Later:

cross-file continuity

cross-exchange lag checks

validation/audit.py

Owns:

Audit dataclass

accumulation logic

No file I/O here. Just state.

3️⃣ pipelines/ — Orchestration only

Rule:

Pipelines wire components together. They contain loops and paths—but no math.

pipelines/normalize_l2.py

Owns:

directory traversal

file opening

calling:

normalization

validation

orderbook

price derivation

After refactor, this file becomes boringly readable.

pipelines/record_live.py

Owns:

which exchanges

which symbols

where data goes

Adapters do not decide this.

4️⃣ scripts/ — Execution entrypoints

Rule:

Zero logic. Zero path hacks. Zero imports from adapters.

These files only do:

from perp_market_microstructure_research.pipelines.normalize_l2 import run
run()


Nothing else.
