import numpy as np
from datetime import datetime
from .. import utils


def date_to_winter_season(date):
    if date.month <= 6:
        y1 = date.year - 1
        y2 = date.year
    else:
        y1 = date.year
        y2 = date.year + 1
    return str(y1)[:] + "/ " + str(y2)[-2:]


def annotate_bars(ax, labels):
    # labels on top of bars
    rects = ax.patches

    ylim = ax.get_ylim()
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            height + np.diff(ylim) / 10,
            label,
            ha="center",
            va="bottom",
            rotation=90,
        )
    ax.set_ylim(None, ylim[1] * 1.5)


def replace_year(dt64, year):
    # convert to timestamp:
    ts = (dt64 - np.datetime64("1970-01-01T00:00:00Z")) / np.timedelta64(1, "s")
    # standard utctime from timestamp
    dt = datetime.utcfromtimestamp(ts)
    # update year
    dt = dt.replace(year=year)
    # convert back to numpy.datetime64:
    return np.datetime64(dt).astype("datetime64[D]")


def fc_dates(reftime=None, hc_year=None, days_since_init=None, data=None):
    if data is not None:
        reftime, hc_year, days_since_init = (
            data.reftime,
            data.hc_year,
            data.days_since_init,
        )
    days_since_init = np.array(days_since_init)
    dates = utils.add_years(reftime, hc_year) + days_since_init
    return dates
