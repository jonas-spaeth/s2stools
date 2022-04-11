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


def mean_and_error(
    da,
    x,
    y,
    hue=None,
    plot_mean=True,
    ci=None,
    ax=None,
    fill_kwargs={},
    line_kwargs={},
    legend_kwargs={},
):
    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = plt.gcf()

    def plot_helper(da, label=None):
        s2stools.plot.fill_between(da, x, y, ax=ax, ci=ci, **fill_kwargs)
        if plot_mean:
            da.mean(y).plot(ax=ax, label=label, **line_kwargs)

    if hue is not None:
        for hue_sel in da[hue]:
            plot_helper(da.sel({hue: hue_sel}), label=hue_sel.values)
        ax.legend(**legend_kwargs)
    else:
        plot_helper(da.sel({hue: hue_sel}), label=hue_sel.values)
    return ax
