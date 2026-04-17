# MLFQ Scheduler for DuckDB
**CS544 — Database Systems Implementation**
**Team:** Neel Gude, Shubh Mishra, Spoorthi Gowda — University of Southern California

---

## Project Overview

This project implements a **Multi-Level Feedback Queue (MLFQ)** task scheduler inside DuckDB's morsel-driven parallel execution engine. The goal is to reduce tail latency for short "mouse" queries when they run concurrently with long-running "elephant" queries.

### The Problem

DuckDB's default scheduler uses a single FIFO concurrent queue. When a large analytical query (elephant) is running, it floods the task queue with thousands of morsels. Short queries (mice) that arrive concurrently must wait behind all those tasks, causing high tail latency (P99).

### Our Solution

We replaced the single-queue scheduler with a **3-level MLFQ**:

- **Q0 (High Priority)** — New queries start here
- **Q1 (Medium Priority)** — Queries demoted after consuming Q0_THRESHOLD morsels
- **Q2 (Low Priority)** — Queries demoted after consuming Q1_THRESHOLD morsels

Worker threads always serve Q0 first, then Q1, then Q2. This ensures short mouse queries stay in Q0 and get CPU priority over demoted elephant queries.

**Starvation prevention:** An aging mechanism promotes tasks from Q1/Q2 back to Q0 if they have waited more than 500ms.

---

## Key Files Changed

| File | Description |
|------|-------------|
| `src/parallel/task_scheduler.cpp` | Core MLFQ implementation |
| `mlfq_benchmark/main.cpp` | TPC-H benchmark driver |
| `mlfq_benchmark/generate_comparison_graph.py` | Graph generation (MLFQ vs Vanilla) |
| `mlfq_benchmark/generate_graph.py` | MLFQ-only graph generation |
| `mlfq_benchmark/vanilla_results.csv` | Saved vanilla DuckDB baseline results |

---

## MLFQ Implementation Details

**File:** `src/parallel/task_scheduler.cpp`

### Architecture

The original `ConcurrentQueue` used `moodycamel::ConcurrentQueue` (a single lock-free queue). We replaced it with three `std::deque` queues (`q0`, `q1`, `q2`) protected by two separate mutexes:

- `queue_lock` — protects the deques (enqueue/dequeue operations)
- `state_lock` — protects per-query morsel counts and priority levels

Using two locks reduces contention: `NotifyTaskComplete` only touches `state_lock`, while `Dequeue` only touches `queue_lock`.

### Demotion Thresholds

```cpp
static constexpr idx_t Q0_THRESHOLD = 1000;  // morsels before Q0 → Q1
static constexpr idx_t Q1_THRESHOLD = 5000;  // morsels before Q1 → Q2
```

At SF=10, TPC-H mouse queries (Q6/Q14/Q22) consume ~600–800 morsels each, so they complete entirely in Q0 (never demoted). The elephant query (Q1) consumes ~10,000+ morsels and gets demoted to Q1 then Q2.

### Per-Query State Reset

A critical design fix: DuckDB assigns query IDs based on the `ProducerToken` (one per connection), not per individual query execution. Without a reset, morsel counts accumulate across successive query executions on the same connection, causing mice to get permanently demoted.

**Fix:** On each `EnqueueBulk` call (which marks the start of a new query or pipeline phase), we reset the morsel count and priority level to 0 for that connection.

```cpp
// Reset on each new EnqueueBulk — new query starting on this connection
query_morsel_counts[bulk_qid] = 0;
query_priority_levels[bulk_qid] = 0;
```

### Aging (Starvation Prevention)

Every 100 dequeue operations, we check the front of Q1 and Q2. If a task has waited more than `AGING_THRESHOLD_MS = 500ms`, it is promoted back to Q0.

### Logging Events

All events are logged to `stderr` for observability:

| Event | Description |
|-------|-------------|
| `[MLFQ] NEW_QUERY` | Connection reset to Q0 (new query starting) |
| `[MLFQ] PROGRESS` | Morsel count update every 10 completions |
| `[MLFQ] DEMOTE` | Query demoted Q0→Q1 or Q1→Q2 |
| `[MLFQ] AGING` | Starved task promoted back to Q0 |

---

## Building the Project

### Prerequisites
- Windows 10/11
- Visual Studio 2022 (with C++ workload)
- CMake 3.21+
- Python 3.x with `pandas`, `matplotlib`, `numpy`

### Build MLFQ DuckDB

```powershell
# From repo root
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_EXTENSIONS="tpch"
cmake --build . --config Release
```

Or open `build/DuckDB.sln` in Visual Studio and build in Release mode.

### Build the Benchmark

```powershell
cd mlfq_benchmark\build_win
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

---

## Running the Benchmark

### Step 1 — Run MLFQ Benchmark

```powershell
cd mlfq_benchmark\build_win\Release
.\mlfq_bench.exe
```

This runs 3 scenarios (each 30 seconds):
- **Baseline** — 10 mouse threads, 0 elephants
- **Mixed Workload** — 10 mouse threads, 2 elephants
- **Heavy Contention** — 20 mouse threads, 8 elephants

Results are saved to `mlfq_latency_results.csv`.

To see MLFQ scheduling events in real time:
```powershell
.\mlfq_bench.exe 2>&1 | Tee-Object -FilePath mlfq_log.txt
```

### Step 2 — Generate Comparison Graph

```powershell
cd mlfq_benchmark
python generate_comparison_graph.py
```

Opens `comparison_latest_graph.png` — a 4-panel chart comparing MLFQ vs Vanilla DuckDB across all 3 scenarios (box plots + P50/P99 bar charts).

### Vanilla Baseline

The vanilla results are pre-saved in `mlfq_benchmark/vanilla_results.csv`. They were generated by running the identical `main.cpp` benchmark against a pristine (unmodified) DuckDB build on the same SF=10 database.

To regenerate vanilla results (requires building the pristine branch):
```powershell
# Switch to pristine branch, build, then:
.\mlfq_bench.exe   # outputs to vanilla_results.csv
```

---

## Benchmark Results (SF=10)

**Workload:** TPC-H Q6, Q14, Q22 (mice) · Q1 (elephant) · Scale Factor 10

| Scenario | Vanilla P99 | MLFQ P99 | Change |
|---|---|---|---|
| Baseline (Mice Only) | 2083ms | ~2100ms | ~same |
| Mixed Workload (2E+10M) | 2166ms | 2825ms | -26% |
| **Heavy Contention (8E+20M)** | **7841ms** | **5884ms** | **+25% better** |

**Key result:** In the Heavy Contention scenario (8 elephant queries + 20 mouse queries competing simultaneously), MLFQ reduces mouse P99 tail latency by **25%** — demonstrating that priority scheduling protects short queries from being starved by long-running analytical workloads.

### Why Mixed Workload Regresses

In the Mixed Workload scenario, all queries start simultaneously. Elephant queries bulk-enqueue ~10,000 tasks into Q0 before demotion kicks in. Since mice's tasks are appended to Q0 after the elephant's tasks (FIFO ordering), they must wait. This is a known limitation of bulk-enqueue scheduling: MLFQ is most effective when short queries arrive *after* elephants have already been demoted.
