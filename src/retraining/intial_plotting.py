import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

df = pd.read_csv('results/retraining/msm_preliminary_results.csv', parse_dates=['date_end'])
retrain_windows = df[df['msm_trigger'] == True]

fig, axes = plt.subplots(3, 1, figsize=(14, 12))
fig.tight_layout(pad=4.0)

# ── Plot 1: Static vs MSM F1 per window ───────────────────
ax1 = axes[0]
ax1.plot(df['date_end'], df['static_f1'], label='Static', linewidth=1.8, color='steelblue')
ax1.plot(df['date_end'], df['msm_f1'],    label='MSM',    linewidth=1.8, color='darkorange')
for _, r in retrain_windows.iterrows():
    ax1.axvline(x=r['date_end'], color='red', linestyle='--', linewidth=1.4, alpha=0.8)
ax1.scatter(retrain_windows['date_end'], retrain_windows['msm_f1'],
            color='red', marker='^', s=80, zorder=5, label='Retrain triggered')
ax1.set_title('Static vs MSM F1 Score per Window', fontsize=13, fontweight='bold')
ax1.set_xlabel('Window end date')
ax1.set_ylabel('Macro F1')
ax1.legend(loc='upper right')
ax1.grid(True, alpha=0.3)

# ── Plot 2: Average F1 bar chart ──────────────────────────
ax2 = axes[1]
means = {
    'Static\n(all windows)':  df['static_f1'].mean(),
    'MSM\n(all windows)':     df['msm_f1'].mean(),
    'Static\n(retrain only)': df.loc[df['msm_trigger'], 'static_f1'].mean(),
    'MSM\n(retrain only)':    df.loc[df['msm_trigger'], 'msm_f1'].mean(),
}
bar_colors = ['steelblue', 'darkorange', 'steelblue', 'darkorange']
bars = ax2.bar(means.keys(), means.values(), color=bar_colors,
               edgecolor='white', linewidth=0.8, width=0.5)
for bar, val in zip(bars, means.values()):
    ax2.text(bar.get_x() + bar.get_width() / 2,
             bar.get_height() + 0.005,
             f'{val:.3f}', ha='center', va='bottom', fontsize=11)
ax2.set_title('Mean F1 Score: Static vs MSM', fontsize=13, fontweight='bold')
ax2.set_ylabel('Mean Macro F1')
ax2.set_ylim(0, max(means.values()) * 1.2)
ax2.grid(True, axis='y', alpha=0.3)
static_patch  = mpatches.Patch(color='steelblue',   label='Static')
msm_patch     = mpatches.Patch(color='darkorange',  label='MSM')
ax2.legend(handles=[static_patch, msm_patch], loc='upper right')

# ── Plot 3: Graph MSM score over time ─────────────────────
ax3 = axes[2]
ax3.fill_between(df['date_end'], df['graph_msm'], alpha=0.15, color='mediumslateblue')
ax3.plot(df['date_end'], df['graph_msm'], linewidth=2, color='mediumslateblue', label='Graph MSM')
ax3.axhline(y=0.6, color='red', linestyle=':', linewidth=1.8, label='τ₁ = 0.6 (retrain threshold)')
for _, r in retrain_windows.iterrows():
    ax3.axvline(x=r['date_end'], color='red', linestyle='--', linewidth=1.4, alpha=0.8)
ax3.set_title('Graph-Level MSM Score per Window', fontsize=13, fontweight='bold')
ax3.set_xlabel('Window end date')
ax3.set_ylabel('MSM Score')
ax3.set_ylim(0.4, 1.05)
ax3.legend(loc='upper right')
ax3.grid(True, alpha=0.3)

plt.savefig('results/retraining/msm_results.png', dpi=150, bbox_inches='tight')
plt.show()
print("Saved to results/retraining/msm_results.png")
