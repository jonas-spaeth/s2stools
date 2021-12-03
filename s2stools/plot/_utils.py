import numpy as np


def symmetric_ylim(ax):
    ylim = ax.get_ylim()
    absylim = np.max(np.abs(ylim))
    ax.set_ylim(-absylim, absylim)
