import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

mlfq_csv    = 'mlfq_latency_results.csv'
vanilla_csv = 'C:/Users/neelg/duckdb-mlfq/mlfq_benchmark/vanilla_sf10_results.csv'
output      = 'comparison_latest_graph.png'

mlfq    = pd.read_csv(mlfq_csv);    mlfq['Latency_ms']    = mlfq['Latency_Microseconds']    / 1000.0
vanilla = pd.read_csv(vanilla_csv); vanilla['Latency_ms'] = vanilla['Latency_Microseconds'] / 1000.0

scenario_order  = ['Baseline_Mice_Only', 'Mixed_Workload', 'Heavy_Contention']
scenario_labels = ['Baseline\n(Mice Only)', 'Mixed\nWorkload', 'Heavy\nContention']

def get_mice(df):
    d = df[df['QueryType'] == 'Mouse'].copy()
    d['Scenario'] = pd.Categorical(d['Scenario'], categories=scenario_order, ordered=True)
    return d.sort_values('Scenario')

mm = get_mice(mlfq)
vm = get_mice(vanilla)

mp99 = mm.groupby('Scenario', observed=False)['Latency_ms'].quantile(0.99).reindex(scenario_order)
vp99 = vm.groupby('Scenario', observed=False)['Latency_ms'].quantile(0.99).reindex(scenario_order)
mp50 = mm.groupby('Scenario', observed=False)['Latency_ms'].quantile(0.50).reindex(scenario_order)
vp50 = vm.groupby('Scenario', observed=False)['Latency_ms'].quantile(0.50).reindex(scenario_order)

fig = plt.figure(figsize=(16, 10))
fig.suptitle('MLFQ vs Vanilla DuckDB — Mouse Query Latency\nTPC-H Q6/Q14/Q22 (mice)  ·  Q1 (elephant)  ·  SF=10', fontsize=14, fontweight='bold')

gs = plt.GridSpec(2, 2, figure=fig, wspace=0.38, hspace=0.48)
box_colors = ['#4CAF82', '#F4845F', '#8FA8C8']

# Top-left: Vanilla box plot
ax1 = fig.add_subplot(gs[0, 0])
vdata = [vm[vm['Scenario']==s]['Latency_ms'].values for s in scenario_order]
bp1 = ax1.boxplot(vdata, patch_artist=True, medianprops=dict(color='black', linewidth=1.5),
                  flierprops=dict(marker='o', markersize=2, linestyle='none'))
for patch, c in zip(bp1['boxes'], box_colors):
    patch.set_facecolor(c); patch.set_alpha(0.85)
ax1.set_yscale('log')
ax1.set_xticks([1,2,3]); ax1.set_xticklabels(scenario_labels, fontsize=9)
ax1.set_ylabel('Latency (ms)'); ax1.set_title('Vanilla DuckDB — Distribution (log scale)')

# Top-right: MLFQ box plot
ax2 = fig.add_subplot(gs[0, 1])
mdata = [mm[mm['Scenario']==s]['Latency_ms'].values for s in scenario_order]
bp2 = ax2.boxplot(mdata, patch_artist=True, medianprops=dict(color='black', linewidth=1.5),
                  flierprops=dict(marker='o', markersize=2, linestyle='none'))
for patch, c in zip(bp2['boxes'], box_colors):
    patch.set_facecolor(c); patch.set_alpha(0.85)
ax2.set_yscale('log')
ax2.set_xticks([1,2,3]); ax2.set_xticklabels(scenario_labels, fontsize=9)
ax2.set_ylabel('Latency (ms)'); ax2.set_title('MLFQ DuckDB — Distribution (log scale)')

# Bottom-left: P50 grouped bar
ax3 = fig.add_subplot(gs[1, 0])
x = np.arange(len(scenario_order)); w = 0.35
bv = ax3.bar(x - w/2, vp50.values, w, label='Vanilla', color='#5B8DB8', alpha=0.9)
bm = ax3.bar(x + w/2, mp50.values, w, label='MLFQ',    color='#E8834A', alpha=0.9)
for bar in list(bv) + list(bm):
    ax3.text(bar.get_x()+bar.get_width()/2, bar.get_height()+10,
             f'{bar.get_height():.0f}ms', ha='center', va='bottom', fontsize=8)
ax3.set_xticks(x); ax3.set_xticklabels(scenario_labels, fontsize=9)
ax3.set_ylabel('P50 Latency (ms)'); ax3.set_title('P50 Median Latency (Mouse Queries)')
ax3.legend(loc='upper left')
ax3.set_ylim(0, max(list(vp50) + list(mp50)) * 1.3)

# Bottom-right: P99 grouped bar
ax4 = fig.add_subplot(gs[1, 1])
bv2 = ax4.bar(x - w/2, vp99.values, w, label='Vanilla', color='#5B8DB8', alpha=0.9)
bm2 = ax4.bar(x + w/2, mp99.values, w, label='MLFQ',    color='#E8834A', alpha=0.9)
for bar in list(bv2) + list(bm2):
    ax4.text(bar.get_x()+bar.get_width()/2, bar.get_height()+10,
             f'{bar.get_height():.0f}ms', ha='center', va='bottom', fontsize=8)
ax4.set_xticks(x); ax4.set_xticklabels(scenario_labels, fontsize=9)
ax4.set_ylabel('P99 Latency (ms)'); ax4.set_title('P99 Tail Latency — Lower is Better')
ax4.legend(loc='upper left')
ax4.set_ylim(0, max(list(vp99) + list(mp99)) * 1.3)

plt.savefig(output, dpi=150, bbox_inches='tight')
print('Saved:', output)

print()
print(f"{'Scenario':<22} | {'Van P50':>8} {'MLFQ P50':>9} | {'Van P99':>8} {'MLFQ P99':>9} {'Improvement':>12}")
print('-' * 75)
for s in scenario_order:
    imp = (vp99[s] - mp99[s]) / vp99[s] * 100
    print(f"{s:<22} | {vp50[s]:>7.0f}ms {mp50[s]:>8.0f}ms | {vp99[s]:>7.0f}ms {mp99[s]:>8.0f}ms  {imp:>+.1f}%")
