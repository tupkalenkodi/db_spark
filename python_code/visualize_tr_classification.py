import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

files = [
    '../data/timing_results/classification_order10_workers2_2026-01-05_07-39-46.json',
    '../data/timing_results/classification_order20_workers2_2026-01-05_07-43-06.json',
    '../data/timing_results/classification_order50_workers2_2026-01-05_07-50-01.json',
    '../data/timing_results/classification_order100_workers2_2026-01-05_08-02-54.json',
    '../data/timing_results/classification_order150_workers2_2026-01-05_08-20-04.json'
]

data_list = []
for f_name in files:
    with open(f_name, 'r') as f:
        content = json.load(f)
        data_list.append({
            'Order': content['order'],
            'Total Time (s)': content['total_time_seconds']
        })

df = pd.DataFrame(data_list).sort_values('Order')

sns.set_theme(style="white")
plt.figure(figsize=(10, 6))

plot = sns.barplot(
    data=df,
    x='Order',
    y='Total Time (s)',
    hue='Order',
    palette='viridis',
    legend=False
)

plot.grid(False)
sns.despine()

plt.title('Classification Execution Time by Graph Order (2 Workers)', fontsize=15, pad=20)
plt.xlabel('Number of Graphs In Millions', fontsize=12)
plt.ylabel('Total Time (seconds)', fontsize=12)

for p in plot.patches:
    if p.get_height() > 0:
        plot.annotate(f'{p.get_height():.1f}s',
                      (p.get_x() + p.get_width() / 2., p.get_height()),
                      ha='center', va='bottom',
                      xytext=(0, 5),
                      textcoords='offset points',
                      fontweight='bold',
                      fontsize=10)

plt.tight_layout()
plt.savefig('../data/visualizations/classification.png')
