import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker


def xaxis_unit_days(ax, multiple=1):
    # Function that formats the axis labels
    def timeTicks(x, pos):
        seconds = x / 10 ** 9  # convert nanoseconds to seconds
        # create datetime object because its string representation is alright
        d = datetime.timedelta(seconds=seconds)
        return str(d.days)

    locator = plt.MultipleLocator(8.64e+13 * multiple)
    ax.xaxis.set_major_locator(locator)
    formatter = matplotlib.ticker.FuncFormatter(timeTicks)
    ax.xaxis.set_major_formatter(formatter)
