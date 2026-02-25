import os
import time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy import stats
from statsmodels.stats.diagnostic import acorr_ljungbox

from tigramite import data_processing as pp
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.lpcmci import LPCMCI
from tigramite.toymodels import structural_causal_processes as toys

from tigramite.independence_tests.parcorr import ParCorr
from tigramite.independence_tests.robust_parcorr import RobustParCorr

np.random.seed(42) # for reproducibility

# read data in
df = pd.read_csv("data/processed/standardized_data.csv",parse_dates=["Date"])
feature_cols = [col for col in df.columns if col != "Date"] # extract feature col names

df = df.dropna(subset=feature_cols).reset_index(drop=True) # drop empty rows after transformation

data = df[feature_cols].to_numpy()
var_names = feature_cols

T, N = data.shape

print(f'    Observations: {T}')
print(f'    Variables: {N}')
print(f'    Variable Names: {var_names}')


# convert to tigramite dataframe
dataframe = pp.DataFrame(
    data=data,
    datatime=np.arange(data.shape[0]),
    var_names=var_names
)





def tau_max_selection():
    print("\n\n\n=====TAU_MAX SELECTION=====")
    # list all ci tests to be used
    lag_ci_tests = {
        'ParCorr': ParCorr(significance='analytic'),
        'RobustParCorr': RobustParCorr(significance='analytic')
    }

    sig_counts = {}

    for ci_name, ci_test in lag_ci_tests.items():
        print(f"\tRunning PCMCI with {ci_name}...")
        pcmci_lag = PCMCI(
            dataframe=dataframe,
            cond_ind_test=ci_test,
            verbosity=0
        )
        
        lag_results = pcmci_lag.get_lagged_dependencies(
            tau_min=1,
            tau_max=21, # one month (trading days)
            val_only=False,
            fdr_method='fdr_bh' # default='none'
        )
        
        counts = []
        
        # iterate over different tau_max values
        for tau_max in range(1, 22):
            count = int(np.sum(lag_results['p_matrix'][:, :, tau_max] < 0.05)) - N # subtract self-links
            counts.append(max(count, 0)) # ensure non-negative counts
            
        sig_counts[ci_name] = counts
        

    # plot results
    fig, ax = plt.subplots(figsize=(12, 4))
    x = np.arange(1, 22)

    width = 0.4

    ax.bar(x - width/2, sig_counts['ParCorr'], width, label='ParCorr', color='blue')
    ax.bar(x + width/2, sig_counts['RobustParCorr'], width, label='RobustParCorr', color='orange')

    ax.set_xlabel('Lag (days)')
    ax.set_ylabel('Number of Significant Dependencies')
    ax.set_title('Significant Dependencies vs Lag for Different CI Tests')
    ax.legend()
    plt.tight_layout()
    plt.show()








def arch_effect_testing():
    print("\n\n\n===== ARCH Effect Testing ======")
    arch_rows=[]
    arch_vars=[]

    print(f'{"Variable":<16} {"Ljung-Box p-value":<20} {"ARCH p-value":<20}')
    print("-" * 60)

    for i, var in enumerate(var_names):
        sq = data[:, i] ** 2
        lb = acorr_ljungbox(
            sq, lags=[10], return_df=True
        )
        lb_stat = lb['lb_stat'].values[0]
        lb_p = lb['lb_pvalue'].values[0]
        has_arch = lb_p < 0.05
        if has_arch:
            arch_vars.append(var)
        flag = "YES" if has_arch else "NO"
        print(f'{var:<16} {lb_p:<20.4e} {flag}')
        
        arch_rows.append({
            'variable': var,
            'ljung_box_stat': lb_stat,
            'ljung_box_p': round(lb_p, 6),
            'has_arch': flag
        })
        
    pd.DataFrame(arch_rows).to_csv("results/arch_effects.csv", index=False)

    if arch_vars:
        print(f'\tARCH effects detected in: {arch_vars}')
    else:
        print('\tNo ARCH effects detected in any variable.')
    
    
    
    
    
def multicollinearity_testing():
    print("\n\n\n====== Multicollinearity Check =====")
    print("  Flagging pairs with |r| > 0.85 as potential conditioning instability.")

    corr_df = pd.DataFrame(data, columns=var_names).corr()
    
    # save results
    corr_df.to_csv("results/correlation_matrix.csv", index=True)

    print("  Full correlation matrix:")
    print("  " + corr_df.round(3).to_string().replace('\n', '\n  '))


    print("\n  High correlation pairs (|r| > 0.85):")
    flagged_pairs = []
    for i in range(len(var_names)):
        for j in range(i + 1, len(var_names)):
            r = corr_df.iloc[i, j]
            if abs(r) > 0.85:
                flagged_pairs.append((var_names[i], var_names[j], round(r, 3)))
                print(f"!!! - {var_names[i]} & {var_names[j]}: r = {r:.3f}")

    # Correlation heatmap for thesis appendix
    fig_corr, ax_corr = plt.subplots(figsize=(10, 8))
    im = ax_corr.imshow(corr_df.values, cmap='RdBu_r', vmin=-1, vmax=1)
    ax_corr.set_xticks(range(N))
    ax_corr.set_yticks(range(N))
    ax_corr.set_xticklabels(var_names, rotation=45, ha='right', fontsize=9)
    ax_corr.set_yticklabels(var_names, fontsize=9)
    for i in range(N):
        for j in range(N):
            ax_corr.text(j, i, f"{corr_df.values[i, j]:.2f}",
                        ha='center', va='center', fontsize=7,
                        color='white' if abs(corr_df.values[i, j]) > 0.6 else 'black')
    plt.colorbar(im, ax=ax_corr, label='Pearson r')
    ax_corr.set_title('Variable Correlation Matrix', fontsize=13)
    plt.tight_layout()
    plt.show()
    


def toy_model_validation():
    print("\n\n\n====== Toy Model Validation =====")

    scenarios = [
        {
            'name'        : 'Standard',
            'description' : 'Strong coefficients (0.4), 1000 obs. Baseline pass/fail check.',
            'T'           : 1000,
            'links_coeffs': {
                0: [((0, -1), 0.7)],
                1: [((1, -1), 0.7), ((0, -1),  0.4)],  # V0 → V1 at lag 1
                2: [((2, -1), 0.7), ((1, -2), -0.4)],  # V1 → V2 at lag 2
                3: [((3, -1), 0.7), ((2, -1),  0.4)]   # V2 → V3 at lag 1
            },
            'true_links'  : {(0, 1, 1), (1, 2, 2), (2, 3, 1)},
            'tau_max'     : 3,
            'recall_threshold' : 0.5   # standard threshold
        },
        {
            'name'        : 'Weak Signal',
            'description' : 'Weak coefficients (0.2), 1000 obs. Tests detection power — '
                            'mimics sparse real financial causal structure.',
            'T'           : 1000,
            'links_coeffs': {
                0: [((0, -1), 0.5)],
                1: [((1, -1), 0.5), ((0, -1),  0.2)],  # V0 → V1 at lag 1 (weak)
                2: [((2, -1), 0.5), ((1, -2), -0.2)],  # V1 → V2 at lag 2 (weak)
                3: [((3, -1), 0.5), ((2, -1),  0.2)]   # V2 → V3 at lag 1 (weak)
            },
            'true_links'  : {(0, 1, 1), (1, 2, 2), (2, 3, 1)},
            'tau_max'     : 3,
            'recall_threshold' : 0.5   # standard threshold
        },
        {
            'name'        : 'Short Window',
            'description' : 'Strong coefficients (0.4), 250 obs (~1 trading year). '
                            'Matches rolling window size — most realistic test.',
            'T'           : 250,
            'links_coeffs': {
                0: [((0, -1), 0.7)],
                1: [((1, -1), 0.7), ((0, -1),  0.4)],  # V0 → V1 at lag 1
                2: [((2, -1), 0.7), ((1, -2), -0.4)],  # V1 → V2 at lag 2
                3: [((3, -1), 0.7), ((2, -1),  0.4)]   # V2 → V3 at lag 1
            },
            'true_links'  : {(0, 1, 1), (1, 2, 2), (2, 3, 1)},
            'tau_max'     : 3,
            'recall_threshold' : 0.5   # standard threshold
        },
        {
            'name'        : 'Short Window + Weak Signal',
            'description' : 'Weak coefficients (0.2), 250 obs. Worst-case scenario — '
                            'some misses expected and acceptable.',
            'T'           : 250,
            'links_coeffs': {
                0: [((0, -1), 0.5)],
                1: [((1, -1), 0.5), ((0, -1),  0.2)],  # V0 → V1 at lag 1 (weak)
                2: [((2, -1), 0.5), ((1, -2), -0.2)],  # V1 → V2 at lag 2 (weak)
                3: [((3, -1), 0.5), ((2, -1),  0.2)]   # V2 → V3 at lag 1 (weak)
            },
            'true_links'  : {(0, 1, 1), (1, 2, 2), (2, 3, 1)},
            'tau_max'     : 3,
            'recall_threshold' : 0.33  # relaxed — low power expected at 250 obs + weak signal
        },
    ]

    all_rows = []

    for scenario in scenarios:
        name              = scenario['name']
        T                 = scenario['T']
        links_coeffs      = scenario['links_coeffs']
        true_links        = scenario['true_links']
        TOY_TAU_MAX       = scenario['tau_max']
        recall_threshold  = scenario['recall_threshold']

        print(f"\n\n\t── Scenario: {name} ──")
        print(f"\t{scenario['description']}")
        print(f"\tToy data: {T} obs, 4 variables")
        print(f"\tTrue causal links: {true_links}")

        toy_data, _ = toys.var_process(links_coeffs, T=T)
        toy_dataframe = pp.DataFrame(
            data=toy_data,
            datatime=np.arange(T),
            var_names=[f"V{i}" for i in range(4)]
        )

        # tau_max=3: true links go to lag 2, add 1 buffer so the algorithm
        # is not artificially capped exactly at the deepest true lag
        pcmci_toy = PCMCI(
            dataframe=toy_dataframe,
            cond_ind_test=RobustParCorr(significance='analytic'),
            verbosity=0
        )

        res = pcmci_toy.run_pcmciplus(
            tau_min=0,
            tau_max=TOY_TAU_MAX,
            pc_alpha=None,
            fdr_method='fdr_bh'
        )
        tau_min_eval = 1  # evaluate lagged links only against true_links

        # use res['graph'] directly — documentation states this is the correct
        # PCMCI+ output. get_graph_from_pmatrix loses orientation marks.
        graph_toy = res['graph']
        detected = set()

        for i in range(4):              # iterate over variables as potential targets
            for j in range(4):          # iterate over variables as potential sources
                if i == j:              # skip self-links
                    continue
                for tau in range(tau_min_eval, TOY_TAU_MAX + 1):
                    if tau == 0 and i >= j:  # skip lower triangle at tau=0
                        continue
                    if graph_toy[i, j, tau] not in ['', 'x-x']:
                        detected.add((i, j, tau))

        tp = true_links & detected  # correctly found
        fp = detected - true_links  # invented — should not exist
        fn = true_links - detected  # missed — should have been found

        # define metrics
        precision = len(tp) / len(detected)    if detected   else 0.0
        recall    = len(tp) / len(true_links)  if true_links else 0.0
        f1        = (2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0)

        print(f"\n  PCMCI+:")
        print(f"\tTrue Positives  ({len(tp)}): {tp}")
        print(f"\tFalse Positives ({len(fp)}): {fp}")
        print(f"\tFalse Negatives ({len(fn)}): {fn}")
        print(f"\tPrecision: {precision:.3f}  |  Recall: {recall:.3f}  |  F1: {f1:.3f}")

        if recall < recall_threshold:
            print(f"\n\tCRITICAL: recall = {recall:.2f} < {recall_threshold}")
            print(f"\tPipeline is over-pruning real links.")
            print(f"\tCheck pc_alpha setting and tau_max. Do NOT proceed.")

        if precision < 0.5:
            print(f"\n\tCRITICAL: precision = {precision:.2f} < 0.5")
            print(f"\tToo many false positives despite FDR. Investigate CI test.")

        if recall >= recall_threshold and precision >= 0.5:
            print(f"\n\tValidation passed — safe to proceed.")

        all_rows.append({
            'Scenario'  : name,
            'T'         : T,
            'CI_Test'   : 'RobustParCorr',
            'Precision' : round(precision, 3),
            'Recall'    : round(recall, 3),
            'F1'        : round(f1, 3),
            'TP'        : len(tp),
            'FP'        : len(fp),
            'FN'        : len(fn)
        })

    pd.DataFrame(all_rows).to_csv("results/toy_validation.csv", index=False)

if __name__ == "__main__":
    #tau_max_selection()
    # based on the graph we will be using RobustParCorr with tau_max=[3, 5, 7]
    # 3: minimum covering all robust signals
    # 5: signals + buffer
    # 7: upper bound to confirm nothing emerges from longer lags
    #arch_effect_testing()
    #multicollinearity_testing()
    toy_model_validation()