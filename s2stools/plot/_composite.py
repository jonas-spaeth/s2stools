from ._utils import *
from ._format import *
import matplotlib.pyplot as plt
import numpy as np


def composite_overview(ds):
    var_list = list(ds.data_vars)
    n_var = len(var_list)
    fig, axes = plt.subplots(nrows=n_var, figsize=(3, n_var * 1.3), sharex=True)
    axes = np.atleast_1d(axes)
    ds_mean = ds.mean("i")
    for i, v in enumerate(var_list):
        ax = axes[i]
        ds_mean[v].plot(ax=ax, color="k", lw=2)
        xaxis_unit_days(ax, multiple=14)
        ax.set_xlabel("")
        ax.set_title("")
        ax.axhline(0, lw=1, color="gray", zorder=-5)
        ax.axvline(0, lw=1, color="gray", zorder=-5)
        if "nam" in v:
            symmetric_ylim(ax)

    axes[0].set_title("{} events".format(len(ds.i)))
    axes[-1].set_xlabel("lagtime [days]")
    fig.tight_layout()
    return fig, ax
