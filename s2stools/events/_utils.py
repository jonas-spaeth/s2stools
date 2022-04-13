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


def annotate_bars(ax, labels, **kwargs):
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
            **kwargs
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


def fc_dates(reftime=None, hc_year=None, leadtime=None, data=None):
    if data is not None:
        reftime, hc_year, leadtime = (
            data.reftime.values,
            data.hc_year.values,
            data.leadtime.values,
        )
    else:
        reftime = np.array(reftime)
        hc_year = np.array(hc_year)
        leadtime = np.array(leadtime)
    dates = utils.add_years(reftime, hc_year) + leadtime
    return dates


def blocks_where(data, condition):
    """
    Check if condition in 1D numpy array is fulfilled. Return list of event starts, event ends and values during event.
    Example:
    a = np.arange(10, 20)
    blocks_where(a, (a>12) & (a<15))
    -> [[3]], [[5]], [[13,14]]
    Parameters
    ----------
    data : 1D numpy array, with actual data
    condition : 1D numpy array, same shape as data, with True and False entries

    Returns
    -------
    events_starts_list, events_ends_list, event_data_list
    """

    # data = np.concatenate([data, [np.nan]])
    condition = np.concatenate([condition, [False]])

    # Lets say we are looking for a period that data is greater than 2.
    # First, we indicate all those points
    indicators = condition.astype(int)  # now we have [0 0 1 1 0 0]

    # We differentiate that so we will have non-zero wherever data > 2.
    # Note that we concatenate 0 at the beginning.
    indicators_diff = np.concatenate([[condition[0]], indicators[1:] - indicators[:-1]])

    # Now lets seek for those indices
    diff_locations = np.where(indicators_diff != 0)[0]

    # We are resulting in all places that the derivative is non-zero.
    # Those are indices of start and end of events:
    # [event1_start, event1_end, event2_start, ....]
    # So we choose by filtering odd/even places of the resulted vector
    events_starts_list = diff_locations[::2].tolist()
    events_ends_list = diff_locations[1::2].tolist()

    # And now we can also gather the events data by iterating the events.
    event_data_list = []

    for event_start, event_end in zip(events_starts_list, events_ends_list):
        event_data_list.append(data[event_start:event_end])

    return events_starts_list, events_ends_list, event_data_list
