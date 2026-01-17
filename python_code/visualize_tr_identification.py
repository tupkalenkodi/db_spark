import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib.patches import Patch

files = [
    '../data/timing_results/identification_order11_small_workers2_2026-01-05_03-42-17.json',
    '../data/timing_results/identification_order11_large_workers2_2026-01-05_03-48-50.json',
    '../data/timing_results/identification_order12_small_workers2_2026-01-05_08-41-28.json',
    '../data/timing_results/identification_order12_large_workers2_2026-01-05_04-49-13.json'
]

data_list = []
for f_name in files:
    with open(f_name, 'r') as f:
        content = json.load(f)
        timings = content['timings']
        data_list.append({
            'Order': content['order'],
            'Target Size': content['target_label'].capitalize(),
            'CC Time': timings['connected_components_count'],
            'VF2 Time': timings['isomorphism_matching_total'],
            'Other Overhead': content['total_time_seconds'] - (timings['connected_components_count'] + timings['isomorphism_matching_total'])
        })

df = pd.DataFrame(data_list)

sns.set_theme(style="white")
targets = ['Small', 'Large']
orders = [11, 12]
order_labels = {11: "Order 11\n(21 Patterns)", 12: "Order 12\n(110 Patterns)"}

x = np.arange(len(targets))
bar_width, dist_offset = 0.32, 0.2

fig, ax = plt.subplots(figsize=(12, 8))
cmap = plt.get_cmap('viridis')
color_cc, color_vf2, color_other = cmap(0.3), cmap(0.6), cmap(0.85)

for i, order in enumerate(orders):
    subset = df[df['Order'] == order].set_index('Target Size').reindex(targets)
    offset = (i - 0.5) * (dist_offset * 2)
    cc, vf2, other = subset['CC Time'].fillna(0).values, subset['VF2 Time'].fillna(0).values, subset['Other Overhead'].fillna(0).values

    ax.bar(x + offset, cc, bar_width, color=color_cc, alpha=0.9 if i==0 else 0.6)
    ax.bar(x + offset, vf2, bar_width, bottom=cc, color=color_vf2, alpha=0.9 if i==0 else 0.6)
    ax.bar(x + offset, other, bar_width, bottom=cc + vf2, color=color_other, alpha=0.9 if i==0 else 0.6)

    for idx in range(len(targets)):
        curr_x = x[idx] + offset
        if cc[idx] > 15:
            ax.text(curr_x, cc[idx]/2, f'{cc[idx]:.0f}s', ha='center', va='center', color='white', fontweight='bold', fontsize=9)
        if vf2[idx] > 15:
            ax.text(curr_x, cc[idx] + vf2[idx]/2, f'{vf2[idx]:.0f}s', ha='center', va='center', color='white', fontweight='bold', fontsize=9)
        if other[idx] > 15:
            ax.text(curr_x, cc[idx] + vf2[idx] + other[idx]/2, f'{other[idx]:.0f}s', ha='center', va='center', color='black', fontweight='bold', fontsize=9)

        ax.text(curr_x, -12, order_labels[order], ha='center', va='top', fontsize=10, color='#333333')

ax.set_title('Identification Runtime Breakdown: CC vs. VF2 Isomorphism', fontsize=16, pad=30)
ax.set_ylabel('Execution Time (seconds)', fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels([f"Target Graph: {t}" for t in targets], fontsize=12, fontweight='bold')
ax.tick_params(axis='x', which='major', pad=40) # Push main label down to make room for order labels

ax.grid(False)
sns.despine()

legend_elements = [
    Patch(facecolor=color_cc, label='Connected Components (CC)'),
    Patch(facecolor=color_vf2, label='VF2 Isomorphism Matching'),
    Patch(facecolor=color_other, label='Overhead (Load/Spark)'),
    Patch(facecolor='gray', alpha=0.9, label='Order 11 (Solid)'),
    Patch(facecolor='gray', alpha=0.6, label='Order 12 (Faded)')
]
ax.legend(handles=legend_elements, loc='upper left', frameon=False)

plt.tight_layout()
plt.savefig('../data/visualizations/identification.png')
