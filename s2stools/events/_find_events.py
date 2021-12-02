import numpy as np
from .. import utils
from tqdm import tqdm
from ._process_events import *


def find_ssw(u60_10hPa, buffer_start=10, buffer_end=10, require_westwind_start=10):
    buffer_start, buffer_end, require_westwind_start = map(utils.to_timedelta64,
                                                           [buffer_start, buffer_end, require_westwind_start])

    var = u60_10hPa.squeeze()
    var_stacked = var.stack(fc=["reftime", "hc_year", "number"])
    var_stacked_valid = var_stacked.dropna("fc", how="all")
    print("number of:")
    print("\t all forecast combinations: ", len(var_stacked.fc))
    print("\t valid forecast combinations: ", len(var_stacked_valid.fc))

    fc_startwest = (
        var_stacked.where(
            (var_stacked.sel(leadtime=slice("0D", require_westwind_start - 1)) > 0)
        )
            .dropna("fc", how="any")
            .fc
    )

    print("\t forecasts start start with 10 days westwind: ", len(fc_startwest))

    events = []
    for fc in tqdm(fc_startwest[:], desc="Scanning Forecasts for Events"):
        fc_run = var_stacked.sel(fc=fc)

        # find zero crossing
        last_day = fc_run.leadtime.values[-1]
        eastwinddays = fc_run.where(
            fc_run.sel(leadtime=slice(buffer_start, last_day - buffer_end)) < 0,
            drop=True,
        ).leadtime

        if len(eastwinddays) > 0:
            events.append([list(np.atleast_1d(fc)[0]), eastwinddays.values[0]])

    event_dict = eventlist_to_dict(events)

    return event_dict


def find_dynssw(
        u_10hPa_60, buffer_start=10, buffer_end=10, require_westwind_start=10
):
    var_stacked = u_10hPa_60.stack(fc=["reftime", "hc_year", "number"])
    var_stacked_valid = var_stacked.dropna("fc", how="all")
    print("number of:")
    print("\t all forecast combinations: ", len(var_stacked.fc))
    print("\t valid forecast combinations: ", len(var_stacked_valid.fc))

    fc_startwest = (
        var_stacked.where(
            (var_stacked.sel(leadtime=slice(0, require_westwind_start - 1)) > 0)
        )
            .dropna("fc", how="any")
            .fc
    )

    print("\t forecasts start start with 10 days westwind: ", len(fc_startwest))

    events = []
    for fc in tqdm(fc_startwest[:], desc="Scanning Forecasts for Events"):
        fc_run = var_stacked.sel(fc=fc)

        # find zero crossing
        last_day = fc_run.leadtime.values[-1]
        eastwinddays = fc_run.where(
            fc_run.sel(leadtime=slice(buffer_start, last_day - buffer_end)) < 0,
            drop=True,
        ).leadtime

        if len(eastwinddays) > 0:
            TIMEDELTA = 5
            neg_lag = fc_run.sel(leadtime=(eastwinddays[0] - TIMEDELTA))
            pos_lag = fc_run.sel(leadtime=(eastwinddays[0] + TIMEDELTA))

            if neg_lag - pos_lag > 20:
                events.append([list(np.atleast_1d(fc)[0]), eastwinddays.values[0]])

    event_dict = eventlist_to_dict(events)

    return event_dict


def find_nam1000(nam1000, extr_type="", **kwargs):
    var = nam1000  # .squeeze()
    var_stacked = var.stack(fc=["reftime", "hc_year", "number"])
    var_stacked_valid = var_stacked.dropna("fc", how="all")
    print("number of:")
    print("\t all forecast combinations: ", len(var_stacked.fc))
    print("\t valid forecast combinations: ", len(var_stacked_valid.fc))

    events = []
    for fc in tqdm(var_stacked_valid.fc, desc="Scanning Forecasts for Events"):
        fc_run = var_stacked.sel(fc=fc)

        if extr_type == "negative":
            res = s2seventwriter_utils.nam_minus_phase(fc_run, **kwargs)
        elif extr_type == "positive":
            res = s2seventwriter_utils.nam_plus_phase(fc_run, **kwargs)
        else:
            raise Exception("Invalid extr_type. Must be 'negative' or 'positive'.")

        if res:
            events.append([list(np.atleast_1d(res.fc)[0]), int(res.leadtime)])

    event_dict = eventlist_to_dict(events)

    return event_dict
