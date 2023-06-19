import datetime

import matplotlib.pyplot as plt
import matplotlib.ticker

import numpy as np


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
    Standard format is dict(c="k", add_legend=False, alpha=0.5, lw=1.5, hue="number").

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
    """
    print("Not implemented due to cartopy.")
    pass


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

    Warnings
    --------
    warn here

    Examples
    --------
    examples here
    ``plt.plot()``


        $ python example_numpy.py
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
