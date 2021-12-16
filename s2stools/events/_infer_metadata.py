import pandas as pd
from ._utils import *


def n_events_by_lagtime(comp):
    """Counts the number of events contributing to the composite as a function of lag time.

    Args:
        comp (xr.Dataset): Composite Dataset

    Returns:
        list: n events as function of lagtime (length is equal to len(comp.lagtime))
    """
    res = []
    for lag in comp.lagtime:
        n = len(comp.sel(lagtime=lag).dropna("i").i)
        res.append(n)
    return res


def event_dates_from_ds(dataset, model="ecmwf"):
    event_dates = []

    for i in dataset.i[:]:
        ds_sel = dataset.sel(i=i)
        lt_nonan = ds_sel.dropna("lagtime", how="all").lagtime
        if len(lt_nonan) > 0:
            lt = lt_nonan[0]
        else:
            continue
        if model == "ecmwf":
            model_aware_reftime = ds_sel["reftime"]
        elif model == "ukmo":
            model_aware_reftime = (
                ds_sel["reftime"]
                if ds_sel["hc_year"] == 0
                else replace_year(ds_sel["reftime"], 2016)
            )
        else:
            raise Exception("Invalid model: {}. Must be 'ecmwf' or 'ukmo'.".format(model))
        date = fc_dates(
            reftime=model_aware_reftime,
            hc_year=ds_sel["hc_year"],
            leadtime=lt,
        )
        event_dates.append(date)

    return pd.Series(event_dates, dtype="datetime64[D]")


def fc_day_dates_permonth(data):
    data_stacked_days = data.stack(day=("reftime", "hc_year", "leadtime", "number")).dropna(
        "day", how="all"
    )

    fc_day_dates = (
            data_stacked_days.reftime.values.astype("datetime64[D]")
            + data_stacked_days.leadtime.values
    )
    fc_day_dates = pd.Series(fc_day_dates)
    return fc_day_dates.groupby(fc_day_dates.map(lambda t: (t.month))).count()


def event_leadtimes_from_ds(dataset):
    event_dates = []

    for i in dataset.i[:]:
        ds_sel = dataset.sel(i=i)
        lt_nonan = ds_sel.dropna("lagtime").lagtime.values
        if len(lt_nonan) > 0:
            lt = lt_nonan[0]
        else:
            continue
        event_dates.append(np.abs(lt))

    return pd.Series(event_dates, dtype="timedelta64[D]")
