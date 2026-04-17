"""
MLFQ Benchmark — Python runner
Reproduces Shubh's C++ benchmark using official TPC-H queries.
Outputs: mlfq_latency_results.csv + benchmark_graphs.png
"""

import duckdb
import threading
import time
import csv
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import numpy as np

# ── Official TPC-H Queries ────────────────────────────────────────────────────

# ELEPHANT: TPC-H Q1 — full lineitem scan, 6 aggregations
ELEPHANT_QUERY = """
SELECT
    l_returnflag, l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM lineitem
WHERE l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

# MICE: TPC-H Q6, Q14, Q22 — selective, fast, low resource
MOUSE_QUERIES = [
    # Q6: single lineitem scan, tight filters
    """SELECT sum(l_extendedprice * l_discount) AS revenue
       FROM lineitem
       WHERE l_shipdate >= CAST('1994-01-01' AS date)
         AND l_shipdate < CAST('1995-01-01' AS date)
         AND l_discount BETWEEN 0.05 AND 0.07
         AND l_quantity < 24""",

    # Q14: lineitem-part join, 1-month date window
    """SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%'
           THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
           / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
       FROM lineitem, part
       WHERE l_partkey = p_partkey
         AND l_shipdate >= CAST('1995-09-01' AS date)
         AND l_shipdate < CAST('1995-10-01' AS date)""",

    # Q22: customer table aggregation
    """SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
       FROM (SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
             FROM customer
             WHERE substring(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
               AND c_acctbal > (SELECT avg(c_acctbal) FROM customer
                                WHERE c_acctbal > 0.00
                                  AND substring(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17'))
               AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale
       GROUP BY cntrycode ORDER BY cntrycode"""
]

# ── Workers ───────────────────────────────────────────────────────────────────

def run_elephant(db_path, scenario_name, keep_running, results, lock):
    con = duckdb.connect(db_path, read_only=True)
    local = []
    while keep_running[0]:
        start = time.perf_counter()
        con.execute(ELEPHANT_QUERY).fetchall()
        elapsed = (time.perf_counter() - start) * 1e6  # microseconds
        local.append((scenario_name, "Elephant", elapsed))
    with lock:
        results.extend(local)
    con.close()

def run_mouse(db_path, scenario_name, delay_ms, keep_running, results, lock):
    con = duckdb.connect(db_path, read_only=True)
    local = []
    idx = 0
    while keep_running[0]:
        query = MOUSE_QUERIES[idx % 3]
        idx += 1
        start = time.perf_counter()
        con.execute(query).fetchall()
        elapsed = (time.perf_counter() - start) * 1e6
        local.append((scenario_name, "Mouse", elapsed))
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
    with lock:
        results.extend(local)
    con.close()

# ── Scenario runner ───────────────────────────────────────────────────────────

def execute_scenario(db_path, name, duration_s, num_elephants, num_mice, delay_ms):
    print(f"\n--- Starting Scenario: {name} ---")
    print(f"Duration: {duration_s}s | Elephants: {num_elephants} | Mice: {num_mice}")

    keep_running = [True]
    results = []
    lock = threading.Lock()
    threads = []

    for _ in range(num_elephants):
        t = threading.Thread(target=run_elephant,
                             args=(db_path, name, keep_running, results, lock))
        threads.append(t)

    for _ in range(num_mice):
        t = threading.Thread(target=run_mouse,
                             args=(db_path, name, delay_ms, keep_running, results, lock))
        threads.append(t)

    for t in threads:
        t.start()

    time.sleep(duration_s)
    print("Time up. Waiting for threads to finish...")
    keep_running[0] = False

    for t in threads:
        t.join()

    print(f"Scenario {name} complete. {len(results)} measurements collected.")
    return results

# ── Graphing ──────────────────────────────────────────────────────────────────

def create_graphs(csv_path, output_path):
    df = pd.read_csv(csv_path)
    df["Latency_ms"] = df["Latency_Microseconds"] / 1000.0

    # Only mouse queries for the graph (matches Shubh's style)
    mice_df = df[df["QueryType"] == "Mouse"].copy()

    scenario_order = ["Baseline_Mice_Only", "Mixed_Workload", "Heavy_Contention"]
    mice_df["Scenario"] = pd.Categorical(mice_df["Scenario"], categories=scenario_order, ordered=True)
    mice_df = mice_df.sort_values("Scenario")

    # P99 per scenario
    p99 = mice_df.groupby("Scenario")["Latency_ms"].quantile(0.99).reindex(scenario_order)

    # Colors matching Shubh's graph
    box_colors  = ["#4CAF82", "#F4845F", "#8FA8C8"]
    bar_colors  = ["#8B1A1A", "#D2704A", "#E8A882"]

    fig = plt.figure(figsize=(14, 6))
    fig.suptitle("MLFQ Task Scheduler Benchmark: Interactive Query (Mouse) Latency",
                 fontsize=14, fontweight="bold")

    gs = gridspec.GridSpec(1, 2, figure=fig, wspace=0.35)

    # ── Left: Box plot (log scale) ─────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0])
    data_by_scenario = [mice_df[mice_df["Scenario"] == s]["Latency_ms"].values
                        for s in scenario_order]

    bp = ax1.boxplot(data_by_scenario, patch_artist=True,
                     flierprops=dict(marker='o', markersize=2, linestyle='none'),
                     medianprops=dict(color='black', linewidth=1.5))
    for patch, color in zip(bp['boxes'], box_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.85)

    ax1.set_yscale('log')
    ax1.set_xticks([1, 2, 3])
    ax1.set_xticklabels(scenario_order, rotation=15, ha='right', fontsize=9)
    ax1.set_ylabel("Latency (ms)")
    ax1.set_xlabel("Workload Scenario")
    ax1.set_title("Latency Distribution (Log Scale)")

    # ── Right: P99 bar chart ───────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[1])
    bar_order = p99.sort_values(ascending=False).index.tolist()
    bar_vals  = p99.reindex(bar_order).values
    bar_col   = [bar_colors[scenario_order.index(s)] for s in bar_order]

    bars = ax2.bar(bar_order, bar_vals, color=bar_col, width=0.5)
    for bar, val in zip(bars, bar_vals):
        ax2.text(bar.get_x() + bar.get_width() / 2, val + 0.005,
                 f"{val:.2f} ms", ha='center', va='bottom', fontsize=9)

    ax2.set_ylabel("P99 Latency (ms)")
    ax2.set_xlabel("Workload Scenario")
    ax2.set_title("P99 Tail Latency (Lower is Better)")
    ax2.set_xticks(range(len(bar_order)))
    ax2.set_xticklabels(bar_order, rotation=15, ha='right', fontsize=9)
    ax2.set_ylim(0, max(bar_vals) * 1.2)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\nGraph saved to {output_path}")
    plt.show()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    DB_PATH    = "tpch_sf1.duckdb"
    CSV_PATH   = "mlfq_latency_results.csv"
    GRAPH_PATH = "benchmark_graphs.png"
    RUN_TIME   = 30  # seconds per scenario

    # Setup DB if needed
    if not os.path.exists(DB_PATH):
        print("Generating TPC-H SF=1 data...")
        con = duckdb.connect(DB_PATH)
        con.execute("INSTALL tpch; LOAD tpch")
        con.execute("CALL dbgen(sf=1)")
        con.close()
        print("Done.")

    # Write CSV header
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Scenario", "QueryType", "Latency_Microseconds"])

    all_results = []

    # Scenario 1: Baseline — mice only, no elephants
    all_results += execute_scenario(DB_PATH, "Baseline_Mice_Only", RUN_TIME, 0, 10, 50)

    # Scenario 2: Mixed workload — 2 elephants + 10 mice
    all_results += execute_scenario(DB_PATH, "Mixed_Workload", RUN_TIME, 2, 10, 50)

    # Scenario 3: Heavy contention — 8 elephants + 20 mice
    all_results += execute_scenario(DB_PATH, "Heavy_Contention", RUN_TIME, 8, 20, 10)

    # Write all results to CSV
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(all_results)

    print(f"\nAll results saved to {CSV_PATH}")
    print(f"Total measurements: {len(all_results)}")

    # Generate graphs
    create_graphs(CSV_PATH, GRAPH_PATH)

if __name__ == "__main__":
    main()
