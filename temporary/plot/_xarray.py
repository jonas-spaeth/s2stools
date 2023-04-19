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


# def plot_dataarray_map(dataarray, ax=None, **plot_kws):
#     if ax is None:
#         plt.figure(figsize=(10, 5))
# 
#     p = dataarray.plot.contourf(
#         subplot_kws=dict(projection=ccrs.PlateCarree()),
#         transform=ccrs.PlateCarree(),
#         **plot_kws
#     )
# 
#     p.axes.coastlines()
#     gl = p.axes.gridlines(
#         draw_labels=True,
#         dms=True,
#         x_inline=False,
#         y_inline=False,
#         alpha=0.3,
#     )
#     gl.top_labels = False
#     gl.right_labels = False
#     return p
# 


def plot_dots_where(
    dataarray, condition, every_nth_lon=1, every_nth_lat=1, **scatter_kws
):
    """
    Scatter Points where condition is fulfilled.
    
    Parameters
    ----------
    dataarray : xr.DataArray
        Data Field which is used for condition.
    condition : lambda expression
        For example lambda x: x > 0 would scatter points where `dataarray` is positive. 
    every_nth_lon : int
        Skip every `every_nth_lon` th longitude point when scatter plotting. 
    every_nth_lat : int
        Skip every `every_nth_lat` th latitude point when scatter plotting.
    scatter_kws : dict
        kwargs passed to matplotlib's scatter
        
    """
    # coarsen data to low resolution
    data_lr = dataarray.coarsen(
        lon=every_nth_lon, lat=every_nth_lat, boundary="trim"
    ).mean()
    # stack lon and lat
    data_stacked = data_lr.stack(gridpoint=("lon", "lat"))
    # create mask based on condition
    mask = data_stacked.where(condition(data_stacked), drop=True)
    # scatter plot
    plt.scatter(mask.lon, mask.lat, **scatter_kws)