import numpy as np
from s2stools import utils
from tqdm import tqdm
from ._process_events import *
import xarray as xr
from ._utils import *


def find_ssw(u60_10hPa, buffer_start=10, buffer_end=10, require_westwind_start=10):
    """
    Find Sudden Stratospheric Warmings in S2S forecast.

    Parameters
    ----------
    u60_10hPa : xr.DataArray
        zonal wind at 60N 10hPa, requires dimensions `reftime`, `hc_year`, `number`
    buffer_start : int
        When are events allowed to happen the earliest?
    buffer_end : int
        When are events allowed to happen the latest?
    require_westwind_start : int
        How many days should u60 start positive?

    Returns
    -------
    list

    See Also
    --------
    `s2stools.events.composite_from_eventlist`
    """
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

    print(f"\t forecasts start start with {require_westwind_start} days westwind: {len(fc_startwest)}")

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
    """
    DEPRECATED
    """
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


def threshold_exceedance_in_forecasts(forecasts: xr.DataArray, threshold: float, above=True, event_end_to_nan=True):
    """

    Parameters
    ----------
    forecasts : data with dimensions "reftime", "hc_year", "number", "leadtime"
    threshold : to define events
    above : whether to check above or below threshold to define events
    event_end_to_nan : if True, then whenever event is still ongoing when forecast ends, set event_end and duration to NaN

    Returns
    -------
    pd.Dataframe
    """
    # check if necessary coordinates exist
    assert all(c in forecasts.coords for c in ["reftime", "hc_year", "number",
                                               "leadtime"]), "Forecasts must have dimensions reftime, hc_year, number, leadtime"

    # create events DataFrame
    events = pd.DataFrame(
        columns=["reftime", "hc_year", "number", "leadtime_start", "leadtime_end", "duration", "maximum", "minimum"])

    # stack forecasts
    forecasts_stacked = forecasts.stack(fc=["reftime", "hc_year", "number"]).dropna("fc")

    # iterate forecasts (transpose such that fc has dimension leadtime)
    for fc in forecasts_stacked.T:
        fc_values = fc.values
        # if above use > threshold, else use < threshold
        if above:
            comparison = fc_values > threshold
        else:
            comparison = fc_values < threshold
        # find consecutive elements that fulfill condition: get start indices, end indices, values during exceedance
        starts, ends, values = blocks_where(fc_values, comparison)

        n_events = len(starts)
        if n_events == 0:
            continue
        # extract forecast information
        reftime, hc_year, number = fc.fc.values.item()

        # append events to the dataframe
        for i in range(n_events):
            start = starts[i]
            end = ends[i]
            duration = end - start
            # if forecast ends when event is still ongoing
            if (end >= len(fc)) & event_end_to_nan:
                end = np.nan
                duration = np.nan
            events = events.append(
                dict(reftime=reftime, hc_year=hc_year, number=number, leadtime_start=start, leadtime_end=end,
                     duration=duration,
                     maximum=values[i].max(), minimum=values[i].min()), ignore_index=True)
    return events
