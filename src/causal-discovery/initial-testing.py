import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from tigramite import data_processing as pp
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests.parcorr import ParCorr

df = pd.read_csv("data/processed/standardized_data.csv",parse_dates=["Date"])

feature_cols = [col for col in df.columns if col != "Date"]
df = df.dropna(subset=feature_cols).reset_index(drop=True)

data = df[feature_cols].to_numpy()
var_names = feature_cols

print(f"Shape: {data.shape}")
print(f"Variables: {var_names}")

dataframe = pp.DataFrame(
    data=data,
    datatime=np.arange(data.shape[0]),
    var_names=var_names
)

pcmci = PCMCI(
    dataframe=dataframe,
    cond_ind_test=ParCorr(significance='analytic'),
    verbosity=0
)

pcmci.get_lagged_dependencies()

results = pcmci.run_pcmciplus(
    tau_min=0,
    tau_max=5,
    pc_alpha=0.05,
    fdr_method='fdr_bh'
)

print(results)
p_matrix = results['p_matrix']
val_matrix = results['val_matrix']


pcmci.print_significant_links(
    p_matrix=p_matrix,
    val_matrix=val_matrix,
    alpha_level=0.05
)

graph = pcmci.get_graph_from_pmatrix(
    p_matrix=p_matrix,
    alpha_level=0.05,
    tau_min=0,
    tau_max=5
)


tp.plot_graph(
    graph=graph,
    var_names=var_names,
    link_colorbar_label='lag',
    figsize=(10, 10)
)

plt.show()