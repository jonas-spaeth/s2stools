import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker
from cartopy.util import add_cyclic_point
import numpy as np


def xaxis_unit_days(ax=None, multiple=7, minor_multiple=1):
    """
    Convert nanoseconds on x-axis to days.

    Parameters
    ----------
    ax : plt.Axis
        Axis.
    multiple : int
        Defaults to every 7 days.
    minor_multiple : int
        Defaults to every 1 days.

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
    standard_format = dict(c="k", add_legend=False, alpha=0.5, lw=1.5, hue="number")
    return standard_format | dict(kwargs)


def cyclic_xyz(field, longitude_name="longitude", latitude_name="latitude"):
    field = field.transpose(latitude_name, longitude_name)
    lon = field.longitude
    lat = field.latitude
    adj_field, adj_lon = add_cyclic_point(field, coord=lon)
    return adj_lon, lat, adj_field


def xlim_days(leftlim=None, rightlim=None):
    def _to_nanoseconds(x):
        return np.timedelta64(x, "D").astype("timedelta64[ns]").astype("float")

    if leftlim is not None:
        leftlim = _to_nanoseconds(leftlim)
    if rightlim is not None:
        rightlim = _to_nanoseconds(rightlim)
    return (leftlim, rightlim)
