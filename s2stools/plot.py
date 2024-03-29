import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np
import xarray as xr
import matplotlib.patches as mpatches


def xaxis_unit_days(ax=None, multiple=7, minor_multiple=1):
    """
    Convert nanoseconds on x-axis to days.

    Parameters
    ----------
    ax : plt.Axis
    multiple : int
        Defaults to every 7 days.
    minor_multiple : int
        Defaults to every 1 day.

    """

    if ax is None:
        ax = plt.gca()

    # Function that formats the axis labels
    def timeTicks(x, pos):
        seconds = x / 10**9  # convert nanoseconds to seconds
        # create datetime object because its string representation is alright
        d = datetime.timedelta(seconds=seconds)
        return str(d.days)

    locator = plt.MultipleLocator(8.64e13 * multiple)
    ax.xaxis.set_major_locator(locator)
    min_locator = plt.MultipleLocator(8.64e13 * minor_multiple)
    ax.xaxis.set_minor_locator(min_locator)
    formatter = matplotlib.ticker.FuncFormatter(timeTicks)
    ax.xaxis.set_major_formatter(formatter)
    xlab = ax.get_xlabel()
    ax.set_xlabel(f"{xlab.capitalize()} [Days]")


def spaghetti(**kwargs):
    """
    Some standard keyword parameters to produce a ensemble forecast spaghetti plot.
    Standard format is ``dict(c="k", add_legend=False, alpha=0.5, lw=1.5, hue="number")``.

    Parameters
    ----------
    kwargs : mappable
        keyword arguments (optional)

    Returns
    -------
    dict
        Spaghetti keyword dictionary
    """
    standard_format = dict(c="k", add_legend=False, alpha=0.5, lw=1.5, hue="number")
    return standard_format | dict(kwargs)


def cyclic_xyz(field, longitude_name="longitude", latitude_name="latitude"):
    """
    Add cyclic point to longitude such that vertical white line gets removed on contour plots.
    Use as example below.

    Parameters
    ----------
    field : xr.DataArray
    longitude_name : str
        Defaults to 'longitude'
    latitude_name : str
        Defaults to 'latitude'

    Returns
    -------
    tuple
        x, y, z DataArrays

    Examples
    --------
    >>> fig, ax = plt.subplots()
    >>> ax.contourf(*dataarray, cmap='coolwarm')

    Warnings
    --------
    Don't forget the star expression for unpacking.

    Warnings
    --------
    Requires ``cartopy`` (which is not a formal dependency of ``s2stools``.
    """
    try:
        from cartopy.util import add_cyclic_point
    except:
        print("This function requires cartopy. Consider: pip install cartopy")
    else:
        field = field.transpose(latitude_name, longitude_name)
        lon = field.longitude
        lat = field.latitude
        adj_field, adj_lon = add_cyclic_point(field, coord=lon)
        return adj_lon, lat, adj_field


def xlim_days(ax=None, leftlim=None, rightlim=None):
    """
    Adjust limits.

    Parameters
    ----------
    ax : plt.Axis
        apply limit to a given axis
    leftlim : float
        day left limit
    rightlim : float
        day right limit

    Returns
    -------
    (leftlim, rightlim)

    """

    def _to_nanoseconds(x):
        return np.timedelta64(x, "D").astype("timedelta64[ns]").astype("float")

    if leftlim is not None:
        leftlim = _to_nanoseconds(leftlim)
    if rightlim is not None:
        rightlim = _to_nanoseconds(rightlim)
    ax = ax if ax is not None else plt.gca()
    ax.set_xlim(leftlim, rightlim)
    return (leftlim, rightlim)


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


# def mean_and_error(
#         da,
#         x,
#         y,
#         hue=None,
#         plot_mean=True,
#         ci=None,
#         ax=None,
#         fill_kwargs={},
#         line_kwargs={},
#         legend_kwargs={},
# ):
#     if ax is None:
#         fig, ax = plt.subplots()
#     else:
#         fig = plt.gcf()
#
#     def plot_helper(da, label=None):
#         s2stools.plot.fill_between(da, x, y, ax=ax, ci=ci, **fill_kwargs)
#         if plot_mean:
#             da.mean(y).plot(ax=ax, label=label, **line_kwargs)
#
#     if hue is not None:
#         for hue_sel in da[hue]:
#             plot_helper(da.sel({hue: hue_sel}), label=hue_sel.values)
#         ax.legend(**legend_kwargs)
#     else:
#         plot_helper(da.sel({hue: hue_sel}), label=hue_sel.values)
#     return ax


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


def composite_overview(ds: xr.Dataset):
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
        # if "nam" in v:
        #     symmetric_ylim(ax)

    axes[0].set_title("{} events".format(len(ds.i)))
    axes[-1].set_xlabel("lagtime [days]")
    fig.tight_layout()
    return fig, ax


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


def add_map(ax, **kwargs):
    """
    Add a nicely formatted map to a proplot axis.
    Works only with `proplot`  https://proplot.readthedocs.io/en/stable/.

    Parameters
    ----------
    ax : proplot axis
    kwargs : dict
        parameters passed to ax.format(...). overwrites default parameters.

    Returns
    -------

    """
    default_kwargs = dict(
        coast=True,
        land=True,
        landcolor="gray1",
        landalpha=0.2,
        landzorder=-100,
        coastcolor="gray7",
        coastlinewidth=0.4,
        coastzorder=100,
    )
    plotting_kwargs = default_kwargs | kwargs
    ax.format(**plotting_kwargs)


def add_box(dict_lon_lat_slice, ax, **kwargs):
    """
    Add a box to a map projection plot.

    Parameters
    ----------
    dict_lon_lat_slice : dict
        E.g. {"longitude": slice(0, 180), "latitude": slice(40, 60)}
    ax : axis
        matplotlib axis
    kwargs : dict
        additional keyword arguments to format/ overwrite appearance, e.g.: facecolor, edgecolor, lw, zorder

    Returns
    -------

    Warnings
    --------
    Requires ``cartopy`` (which is not a formal dependency of ``s2stools``.
    """

    try:
        import cartopy.crs as ccrs
    except:
        print("This function requires cartopy. Consider: pip install cartopy")
    else:
        box = dict_lon_lat_slice
        width = box["longitude"].stop - box["longitude"].start
        height = box["latitude"].start - box["latitude"].stop
        x, y = box["longitude"].start, box["latitude"].stop

        plot_kwargs = dict(
            facecolor=(0, 0, 1, 0.05),
            edgecolor="k",
            lw=1,
            zorder=10,
        ) | dict(kwargs)

        ax.add_patch(
            mpatches.Rectangle(
                xy=[x, y],
                width=width,
                height=height,
                transform=ccrs.PlateCarree(),
                **plot_kwargs,
            )
        )


def atlantic_map_subplot_kws(lonlim=None, latlim=None, lat0=None, lon0=None):
    if lonlim is None:
        lonlim = (-100, 30)

    if latlim is None:
        latlim = (90, 30)

    if lat0 is None:
        lat0 = 30

    if lon0 is None:
        lon0 = -20

    kws = dict(
        proj="nsper", proj_kw=dict(lat0=lat0, lon0=lon0), lonlim=lonlim, latlim=latlim
    )

    return kws


def cmap_spread(less_dark_blue=0, less_dark_red=0):
    """
    Colormap that I use often for ensemble variance anomalies.

    Parameters
    ----------
    less_dark_blue : float
        If the blue is too dark, setting this parameter to >0 but <.5 results in less dark blue.
    less_dark_red : float
        If the red is too dark, setting this parameter to >0 but <.5 results in less dark red.

    Returns
    -------
    pplt.Colormap
    """
    try:
        import proplot as pplt
    except:
        ImportError("requires proplot. consider: pip install proplot")
    else:
        cm = pplt.Colormap(
            "Blues5_r", (1, 1, 1, 0), "Reds2", ratios=[3, 1, 3], name="cmap_opaque"
        )
        if (less_dark_red == 0) & (less_dark_blue == 0):
            return cm
        else:
            return pplt.Colormap(
                pplt.Colormap(cm, left=less_dark_blue, right=0.45),
                pplt.Colormap(cm, left=0.55, right=1 - less_dark_red),
                ratios=[1, 1],
            )


def cmap_hotcold():
    try:
        import proplot as pplt
    except:
        ImportError("requires proplot. consider: pip install proplot")
    else:
        return pplt.Colormap(
            pplt.Colormap("HotCold_r", right=0.5),
            (1, 1, 1, 0),
            pplt.Colormap("HotCold_r", left=0.5),
            ratios=[20, 1, 20],
        )


def legend_colored_labels_no_lines(ax, fontweight="bold", **kwargs):
    """
    Similar to `ax.legend()`, but instead of drawing lines and putting labels next to them, just write the text in the
    right color.

    Parameters
    ----------
    ax : matplotlib axis or proplot axis
    fontweight : str
        Defaults to "bold".
    kwargs : dict
        additional keyword arguments passed to ax.legend()

    Returns
    -------
    legend
    """
    default_kwargs = dict(columnspacing=1, handletextpad=-2)
    kwargs = default_kwargs | kwargs
    legend = ax.legend(**kwargs)

    # Get the legend handles and labels
    handles = legend.legendHandles
    labels = [text.get_text() for text in legend.get_texts()]

    # Remove the lines from the legend
    for handle in legend.legendHandles:
        handle.set_visible(False)

    # Set the text color of legend labels to match the colors of the lines
    for text, handle in zip(legend.get_texts(), handles):
        if isinstance(handle, plt.Line2D):
            text.set_color(handle.get_color())
        elif isinstance(handle, plt.Rectangle):
            text.set_color(handle.get_facecolor())

        # Set the font weight
        text.set_fontweight(fontweight)

    return legend


def set_xticks_with_colored_labels(ax, xticks, xticklabels, colors, fontweight="bold"):
    """
    Set x-axis ticks with colored labels.

    Parameters
    ----------
    ax : axis
        The axis object
    xticks : list
    xticklabels : list
    colors : list
    fontweight : str
        Defaults to "bold"

    Returns
    -------
    None
    """
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels, weight=fontweight)

    for i, label in enumerate(ax.get_xticklabels()):
        label.set_color(colors[i])
        label.set_fontweight(fontweight)


def set_yticks_with_colored_labels(ax, yticks, yticklabels, colors, fontweight="bold"):
    """
    Set x-axis ticks with colored labels.

    Parameters
    ----------
    ax : axis
        The axis object
    yticks : list
    yticklabels : list
    colors : list
    fontweight : str
        Defaults to "bold"

    Returns
    -------
    None
    """
    ax.set_yticks(yticks)
    ax.set_yticklabels(yticklabels, weight=fontweight)

    for i, label in enumerate(ax.get_yticklabels()):
        label.set_color(colors[i])
        label.set_fontweight(fontweight)


def add_map_inset(
    ax,
    loc="ul",
    size=0.3,
    proj="cyl",
    lonlim=(-180, 180),
    latlim=(0, 90),
    inset_kw={},
    draw_box=None,
    box_kw={},
    format_kw={},
):
    """
    Add a small map to an existing axis. Designed to indicate geographic location of some other analysis.

    Parameters
    ----------
    ax : proplot.axes.Axes
        axis to which map should be added
    loc : str
        location of map, must be one of "ul", "ur", "ll", "lr"
    size : float
        size of map in relative units of axis object, defaults to 0.3.
    proj : str
        map projection
    lonlim : 2-tuple of float
        longitude limits for map
    latlim : 2-tuple of float
        latitude limits for map
    inset_kw : dict
        Addictional keyword arguments passed to ax.inset()
    draw_box : dict
        Dictionary of form {'longitude': slice(lon1, lon2), 'latitude': slice(lat1, lat2)} corresponding to location of
        box. Defaults to None, in which case no box is drawn.
    box_kw : dict
        Addictional keyword arguments passed to s2stools.plot.add_box()
    format_kw : dict
        Addictional keyword arguments passed to ax.format()

    Returns
    -------
    proplot axis of inset
    """

    delta = 0.01
    width = size
    height = size * 0.75
    if loc == "ul":
        bounds = (delta, 1 - height - delta, width, height)
    elif loc == "ur":
        bounds = (1 - width - delta, 1 - height - delta, width, height)
    elif loc == "ll":
        bounds = (delta, delta, width, height)
    elif loc == "lr":
        bounds = (1 - width - delta, delta, width, height)
    else:
        raise ValueError("loc must be one of 'ul', 'ur', 'll', 'lr'")

    inset_kw_default = dict(
        bounds=bounds, proj=proj, lonlim=lonlim, latlim=latlim, zorder=-5
    )
    inset_kw = inset_kw_default | inset_kw
    ax_inset = ax.inset(**inset_kw)
    if add_box:
        assert isinstance(draw_box, dict), (
            "add_box must be dictionary of form"
            "{'longitude': slice(lon1, lon2), 'latitude': slice(lat1, lat2)}"
        )
        add_box(draw_box, ax_inset, **box_kw)
    format_kw_default = dict(coast=True, lw=0, grid=False)
    format_kw = format_kw_default | format_kw
    ax_inset.format(**format_kw)
    return ax_inset
