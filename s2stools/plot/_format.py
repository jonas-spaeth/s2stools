import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker


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
        seconds = x / 10 ** 9  # convert nanoseconds to seconds
        # create datetime object because its string representation is alright
        d = datetime.timedelta(seconds=seconds)
        return str(d.days)

    locator = plt.MultipleLocator(8.64e+13 * multiple)
    ax.xaxis.set_major_locator(locator)
    min_locator = plt.MultipleLocator(8.64e+13 * minor_multiple)
    ax.xaxis.set_minor_locator(min_locator)
    formatter = matplotlib.ticker.FuncFormatter(timeTicks)
    ax.xaxis.set_major_formatter(formatter)
    xlab = ax.get_xlabel()
    ax.set_xlabel(f"{xlab.capitalize()} [Days]")


def spaghetti(**kwargs):
    standard_format = dict(c="k", add_legend=False, alpha=.5, lw=1.5, hue="number")
    return standard_format | dict(kwargs)