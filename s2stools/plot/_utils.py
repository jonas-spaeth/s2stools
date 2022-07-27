import numpy as np


def symmetric_ylim(ax):
    """
    For example, change y limits from -3, 1 to -3, 3. It will be symmetric around 0.

    Parameters
    ----------
    ax : plt.Axis
        Axis.

    """
    ylim = ax.get_ylim()
    absylim = np.max(np.abs(ylim))
    ax.set_ylim(-absylim, absylim)
