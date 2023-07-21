import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np
import xarray as xr
import matplotlib.patches as mpatches

# from cartopy.util import add_cyclic_point


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
        seconds = x / 10 ** 9  # convert nanoseconds to seconds
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
                **plot_kwargs
            )
        )
