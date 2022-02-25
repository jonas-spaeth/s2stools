import matplotlib.pyplot as plt


def fill_between(dataarray, x, y, ax=None, ci=None, **kwargs):
    """
    wraps matplotlib's fill_between for DataArrays
    Parameters
    ----------
    dataarray : xr.DataArray
    x : str
        coordinate name
    y : str
        coordinate name
    ci : (tuple)
        quantiles
    ax : plt.axis
        axis to draw plot, if None call plt.fill_between()
    kwargs : dict
        kwargs passed to matplotlib's fill_between
    Returns
    -------
    void
    """
    x_vals = dataarray[x]
    if ci is None:
        y1_vals = dataarray.min(y)
        y2_vals = dataarray.max(y)
        y_q = [y1_vals, y2_vals]
    else:
        assert len(ci) == 2, "ci must be a tuple consisting of two floats"
        y_q = dataarray.quantile(q=ci, dim=y)
    if ax is None:
        plt.fill_between(x_vals, *y_q, **kwargs)
    else:
        ax.fill_between(x_vals, *y_q, **kwargs)
