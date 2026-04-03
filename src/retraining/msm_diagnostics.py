import os
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap

os.makedirs("results", exist_ok=True)

# load graphs 
print("Loading causal graphs...")
with open("data/causal_graphs.pkl", "rb") as f:
    all_graphs = pickle.load(f)
print(f"Total windows: {len(all_graphs)}")


# ----- MSM FUNCTIONS -----
# copied from retrainer
def compute_graph_msm(all_graphs, w, lookback):
    start = max(0, w - lookback + 1)
    recent_windows = all_graphs[start: w + 1]
    n = len(recent_windows)

    all_edges = set()
    for g in recent_windows:
        all_edges |= g["edges"]

    if not all_edges:
        return 0.0, {}

    edge_scores = {
        e: sum(1 for g in recent_windows if e in g["edges"]) / n
        for e in all_edges
    }
    return np.mean(list(edge_scores.values())), edge_scores


def compute_msm_series(all_graphs, lookback):
    """Return (window_indices, date_labels, msm_values)."""
    idxs, dates, msm_vals = [], [], []
    for w, g in enumerate(all_graphs):
        idxs.append(w)
        dates.append(g.get("date_end", str(w)))
        if w < lookback:
            msm_vals.append(np.nan)
        else:
            msm, _ = compute_graph_msm(all_graphs, w, lookback)
            msm_vals.append(msm)
    return idxs, dates, msm_vals


def simulate_triggers(msm_vals, tau_1, tau_2, cooldown=3):
    '''
    Replay MSMRetrainer.should_retrain() simulation
    '''
    provisional = False
    last_retrain = -999
    retrains = signals = 0

    for w, msm in enumerate(msm_vals):
        if np.isnan(msm):
            continue
        in_cd = (w - last_retrain) < cooldown
        signal = False

        if msm < tau_1 and not provisional:
            provisional = True
        elif provisional and msm < tau_2:
            provisional = False
            signal = True
            signals += 1
            if not in_cd:
                retrains += 1
                last_retrain = w
        elif msm >= tau_1:
            provisional = False

    return retrains, signals


# ── 1. MSM over time ──────────────────────────────────────────────────────────
LOOKBACKS = [3, 4, 6, 8, 12]
COLORS    = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#76b7b2"]

fig, axes = plt.subplots(len(LOOKBACKS), 1, figsize=(14, 3.2 * len(LOOKBACKS)), sharex=True)
fig.suptitle("Graph MSM Values Over Time", fontsize=15, fontweight="bold", y=1.01)

for ax, lb, col in zip(axes, LOOKBACKS, COLORS):
    idxs, dates, msm_vals = compute_msm_series(all_graphs, lb)
    valid = [v for v in msm_vals if not np.isnan(v)]

    p25, p50, p75 = np.percentile(valid, [25, 50, 75])
    ax.plot(idxs, msm_vals, color=col, linewidth=1.4, label=f"MSM (lookback={lb})")
    ax.axhline(p25, color="#e15759", linestyle="--", linewidth=1.2, alpha=0.85, label=f"p25 = {p25:.3f}  ← candidate tau_2")
    ax.axhline(p50, color="#f28e2b", linestyle="--", linewidth=1.2, alpha=0.85, label=f"p50 = {p50:.3f}")
    ax.axhline(p75, color="#59a14f", linestyle="--", linewidth=1.2, alpha=0.85, label=f"p75 = {p75:.3f}  ← candidate tau_1")

    ax.set_ylabel("MSM", fontsize=9)
    ax.set_ylim(0, 1.05)
    ax.set_title(f"Lookback = {lb}", fontsize=10, pad=3)
    ax.legend(fontsize=8, loc="upper right", ncol=2)
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)

axes[-1].set_xlabel("Window Index", fontsize=9)
plt.tight_layout()
plt.savefig("results/validation/msm_over_time.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: results/validation/msm_over_time.png")


# ── 2. MSM distribution histograms ───────────────────────────────────────────
fig, axes = plt.subplots(1, len(LOOKBACKS), figsize=(4.2 * len(LOOKBACKS), 4.5))
fig.suptitle("MSM Score Distribution by Lookback", fontsize=13, fontweight="bold")

for ax, lb, col in zip(axes, LOOKBACKS, COLORS):
    _, _, msm_vals = compute_msm_series(all_graphs, lb)
    valid = [v for v in msm_vals if not np.isnan(v)]
    p25, p50, p75 = np.percentile(valid, [25, 50, 75])

    ax.hist(valid, bins=25, color=col, edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.axvline(p25, color="#e15759", linestyle="--", linewidth=1.5, label=f"p25={p25:.3f}")
    ax.axvline(p50, color="#f28e2b", linestyle="--", linewidth=1.5, label=f"p50={p50:.3f}")
    ax.axvline(p75, color="#59a14f", linestyle="--", linewidth=1.5, label=f"p75={p75:.3f}")
    ax.set_title(f"Lookback = {lb}", fontsize=10)
    ax.set_xlabel("MSM Score", fontsize=9)
    ax.set_ylabel("Count", fontsize=9)
    ax.legend(fontsize=8)

plt.tight_layout()
plt.savefig("results/validation/msm_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: results/validation/msm_distribution.png")


# ── 3. Threshold heatmap — for each lookback ─────────────────────────────────
TAU_1_VALS = np.round(np.arange(0.50, 1.01, 0.05), 2)
TAU_2_VALS = np.round(np.arange(0.35, 0.95, 0.05), 2)

fig, axes = plt.subplots(1, len(LOOKBACKS), figsize=(5 * len(LOOKBACKS), 5.5))
fig.suptitle("Retrain Count Heatmap — (tau_1, tau_2) Grid  |  Cooldown = 3", fontsize=13, fontweight="bold")

cmap = LinearSegmentedColormap.from_list("ramp", ["#f7f7f7", "#fdae61", "#d73027"])

for ax, lb in zip(axes, LOOKBACKS):
    _, _, msm_vals = compute_msm_series(all_graphs, lb)

    grid = np.full((len(TAU_1_VALS), len(TAU_2_VALS)), np.nan)
    for i, t1 in enumerate(TAU_1_VALS):
        for j, t2 in enumerate(TAU_2_VALS):
            if t2 >= t1:
                continue
            r, _ = simulate_triggers(msm_vals, t1, t2, cooldown=3)
            grid[i, j] = r

    im = ax.imshow(grid, aspect="auto", cmap=cmap, origin="lower", vmin=0, vmax=np.nanmax(grid) or 1)
    ax.set_xticks(range(len(TAU_2_VALS)))
    ax.set_xticklabels([f"{v:.2f}" for v in TAU_2_VALS], rotation=60, fontsize=7)
    ax.set_yticks(range(len(TAU_1_VALS)))
    ax.set_yticklabels([f"{v:.2f}" for v in TAU_1_VALS], fontsize=7)
    ax.set_xlabel("tau_2  (confirmation)", fontsize=9)
    ax.set_ylabel("tau_1  (alert)", fontsize=9)
    ax.set_title(f"Lookback = {lb}", fontsize=10)

    for i in range(len(TAU_1_VALS)):
        for j in range(len(TAU_2_VALS)):
            val = grid[i, j]
            if not np.isnan(val):
                txt = ax.text(j, i, f"{int(val)}", ha="center", va="center", fontsize=6,
                              color="white" if val > (np.nanmax(grid) or 1) * 0.55 else "#333")

    plt.colorbar(im, ax=ax, label="# retrains", fraction=0.046, pad=0.04)

plt.tight_layout()
plt.savefig("results/validation/msm_threshold_heatmap.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: results/validation/msm_threshold_heatmap.png")


rows = []
for lb in LOOKBACKS:
    _, _, msm_vals = compute_msm_series(all_graphs, lb)
    valid = [v for v in msm_vals if not np.isnan(v)]
    p10, p25, p50, p75, p90 = np.percentile(valid, [10, 25, 50, 75, 90])
    rows.append({
        "lookback": lb,
        "min":    round(np.min(valid),  4),
        "p10":    round(p10,            4),
        "p25":    round(p25,            4),
        "median": round(p50,            4),
        "p75":    round(p75,            4),
        "p90":    round(p90,            4),
        "max":    round(np.max(valid),  4),
        "mean":   round(np.mean(valid), 4),
        "std":    round(np.std(valid),  4),
        "suggested_tau_1": round(p75, 2),
        "suggested_tau_2": round(p50, 2),
    })

df = pd.DataFrame(rows)
df.to_csv("results/validation/msm_summary.csv", index=False)

print("\n=== MSM Distribution Summary ===")
print(df.to_string(index=False))

print("\n=== Suggested Starting Tau Values (data-driven) ===")
for _, row in df.iterrows():
    print(
        f"  lookback={int(row.lookback):2d}:  "
        f"tau_1 ≈ {row.suggested_tau_1:.2f} (p75),  "
        f"tau_2 ≈ {row.suggested_tau_2:.2f} (p50)"
    )

print("\nAll outputs written to results/validation/")