import pandas as pd
from ._utils import *


def n_events_by_lagtime(comp):
    """Counts the number of events contributing to the composite as a function of lag time.

    Args:
        comp (xr.Dataset): Composite Dataset

    Returns:
        list: n events as function of lagtime (length is equal to len(comp.days_since_event))
    """
    res = []
    for lag in comp.days_since_event:
        n = len(comp.sel(days_since_event=lag).dropna("i").i)
        res.append(n)
    return res


def event_dates_from_ds(dataset, model="ecmwf"):
    event_dates = []

    for i in dataset.i[:]:
        ds_sel = dataset.sel(i=i)
        dsi = int(abs(ds_sel.dropna("days_since_event").days_since_event[0]))
        if model == "ecmwf":
            model_aware_reftime = ds_sel["reftime"]
        elif model == "ukmo":
            model_aware_reftime = (
                ds_sel["reftime"]
                if ds_sel["hc_year"] == 0
                else replace_year(ds_sel["reftime"], 2016)
            )
        else:
            raise "Invalid model: ".format(model)
        date = fc_dates(
            reftime=model_aware_reftime,
            hc_year=ds_sel["hc_year"],
            days_since_init=dsi,
        )
        event_dates.append(date)

    return pd.Series(event_dates)

def fc_day_dates_permonth(data):
    data_stacked_days = data.stack(day=("reftime", "hc_year", "days_since_init", "number")).dropna(
        "day", how="all"
    )

    fc_day_dates = (
            data_stacked_days.reftime.values.astype("datetime64[D]")
            + data_stacked_days.days_since_init.values
    )
    fc_day_dates = pd.Series(fc_day_dates)
    return fc_day_dates.groupby(fc_day_dates.map(lambda t: (t.month))).count()


def event_leadtimes_from_ds(dataset):
    print("event_leadtimes_from_ds")
    event_dates = []

    for i in dataset.i[:]:
        ds_sel = dataset.sel(i=i)
        dsi = int(abs(ds_sel.dropna("days_since_event").days_since_event[0]))
        event_dates.append(dsi)

    return pd.Series(event_dates)
