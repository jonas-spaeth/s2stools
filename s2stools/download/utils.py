import pandas as pd
import numpy as np


def only_monday_thursday(dates):
    is_monday_or_thursday = np.isin(dates.day_of_week, [0, 3])
    return dates[is_monday_or_thursday]


def mid_nov_to_end_feb_inits(year_where_winter_starts):
    all_dates = pd.DatetimeIndex([])
    years = np.atleast_1d(year_where_winter_starts)
    for y in years:
        dates = pd.date_range(start=f"{y}-11-16", end=f"{y+1}-02-22")
        all_dates = all_dates.union(only_monday_thursday(dates))
    return all_dates.values.astype("datetime64[D]")