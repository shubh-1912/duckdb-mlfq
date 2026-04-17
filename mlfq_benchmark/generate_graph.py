import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

csv_path = 'mlfq_latency_results.csv'
output = 'mlfq_latest_graph.png'

df = pd.read_csv(csv_path)
df['Latency_ms'] = df['Latency_Microseconds'] / 1000.0
mice_df = df[df['QueryType'] == 'Mouse'].copy()

scenario_order = ['Baseline_Mice_Only', 'Mixed_Workload', 'Heavy_Contention']
scenario_labels = ['Baseline\n(Mice Only)', 'Mixed\nWorkload', 'Heavy\nContention']
mice_df['Scenario'] = pd.Categorical(mice_df['Scenario'], categories=scenario_order, ordered=True)
mice_df = mice_df.sort_values('Scenario')

p99 = mice_df.groupby('Scenario', observed=False)['Latency_ms'].quantile(0.99).reindex(scenario_order)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('MLFQ Benchmark Results — Mouse Query Latency (SF=10)', fontsize=13, fontweight='bold')

data = [mice_df[mice_df['Scenario']==s]['Latency_ms'].values for s in scenario_order]
bp = ax1.boxplot(data, patch_artist=True, medianprops=dict(color='black', linewidth=1.5))
for patch, c in zip(bp['boxes'], ['#4CAF82','#F4845F','#8FA8C8']):
    patch.set_facecolor(c)
    patch.set_alpha(0.85)
ax1.set_yscale('log')
ax1.set_xticks([1,2,3])
ax1.set_xticklabels(scenario_labels, fontsize=9)
ax1.set_ylabel('Latency (ms)')
ax1.set_title('Latency Distribution (log scale)')

bars = ax2.bar(scenario_labels, p99.values, color=['#4CAF82','#F4845F','#8FA8C8'], width=0.5)
for bar, val in zip(bars, p99.values):
    ax2.text(bar.get_x()+bar.get_width()/2, val+10, f'{val:.0f}ms', ha='center', va='bottom', fontsize=9)
ax2.set_ylabel('P99 Latency (ms)')
ax2.set_title('P99 Tail Latency (Lower is Better)')
ax2.set_ylim(0, max(p99.values)*1.25)

plt.tight_layout()
plt.savefig(output, dpi=150, bbox_inches='tight')
print('Saved:', output)
