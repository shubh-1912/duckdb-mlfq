"""
Official TPC-H Benchmark — All 22 Queries
Runs each query 3 times on vanilla DuckDB (SF=10) and reports latency.
Compare with MLFQ results by running against tpch-sf10.db via the C++ bench.
"""

import duckdb
import time
import os
import csv
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

QUERY_DIR = os.path.join(os.path.dirname(__file__), '../extension/tpch/dbgen/queries')
DB_PATH   = os.path.join(os.path.dirname(__file__), 'tpch_sf10.duckdb')
CSV_OUT   = os.path.join(os.path.dirname(__file__), 'tpch_all22_results.csv')
GRAPH_OUT = os.path.join(os.path.dirname(__file__), 'tpch_all22_graph.png')
RUNS      = 3  # number of times to run each query

def load_queries():
    queries = {}
    for i in range(1, 23):
        path = os.path.join(QUERY_DIR, f'q{i:02d}.sql')
        with open(path) as f:
            queries[i] = f.read()
    return queries

def run_benchmark():
    print(f"Connecting to {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)
    queries = load_queries()
    results = []

    print(f"\nRunning all 22 TPC-H queries ({RUNS} runs each) on SF=10...\n")
    print(f"{'Query':<8} {'Run 1':>10} {'Run 2':>10} {'Run 3':>10} {'Best':>10} {'Avg':>10}")
    print('-' * 55)

    for qnum, sql in queries.items():
        times = []
        for run in range(RUNS):
            try:
                t0 = time.perf_counter()
                con.execute(sql).fetchall()
                elapsed = (time.perf_counter() - t0) * 1000  # ms
                times.append(elapsed)
                results.append({'Query': f'Q{qnum:02d}', 'Run': run+1, 'Latency_ms': elapsed})
            except Exception as e:
                print(f"  Q{qnum:02d} Run {run+1} ERROR: {e}")
                times.append(None)

        valid = [t for t in times if t is not None]
        if valid:
            best = min(valid)
            avg  = sum(valid) / len(valid)
            run_strs = [f'{t:>9.1f}ms' if t else f'{"ERROR":>10}' for t in times]
            print(f"Q{qnum:02d}     {'  '.join(run_strs)}  {best:>8.1f}ms  {avg:>8.1f}ms")

    con.close()
    return results

def save_and_plot(results):
    # Save CSV
    with open(CSV_OUT, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Query', 'Run', 'Latency_ms'])
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved to {CSV_OUT}")

    # Build summary
    df = pd.DataFrame(results)
    summary = df.groupby('Query')['Latency_ms'].agg(['min', 'mean', 'max']).reset_index()
    summary.columns = ['Query', 'Best', 'Avg', 'Worst']
    summary = summary.sort_values('Query')

    # Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle('Official TPC-H All 22 Queries — Vanilla DuckDB SF=10', fontsize=14, fontweight='bold')

    x = np.arange(len(summary))
    ax1.bar(x, summary['Best'], color='#4CAF82', alpha=0.85, label='Best')
    ax1.bar(x, summary['Avg'] - summary['Best'], bottom=summary['Best'],
            color='#F4845F', alpha=0.85, label='Avg - Best')
    ax1.set_xticks(x); ax1.set_xticklabels(summary['Query'], rotation=45, fontsize=9)
    ax1.set_ylabel('Latency (ms)'); ax1.set_title('Query Latency — Best vs Average')
    ax1.legend()

    ax2.bar(x, summary['Best'], color='#5B8DB8', alpha=0.9)
    for i, (_, row) in enumerate(summary.iterrows()):
        ax2.text(i, row['Best'] + 50, f"{row['Best']:.0f}", ha='center', va='bottom', fontsize=7)
    ax2.set_yscale('log')
    ax2.set_xticks(x); ax2.set_xticklabels(summary['Query'], rotation=45, fontsize=9)
    ax2.set_ylabel('Latency (ms, log scale)'); ax2.set_title('Best Run Latency per Query (log scale)')

    plt.tight_layout()
    plt.savefig(GRAPH_OUT, dpi=150, bbox_inches='tight')
    print(f"Graph saved to {GRAPH_OUT}")

if __name__ == '__main__':
    results = run_benchmark()
    save_and_plot(results)
