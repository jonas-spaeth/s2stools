import json
import pandas as pd
import xarray as xr
import numpy as np
import utils
from tqdm import tqdm
from glob import glob
from pathlib import Path


def eventlist_to_dict(events):
    """
    events should have the shape:
    [
        ((reftime, hc_year, number), leadtime),
        ((reftime, hc_year, number), leadtime),
        ...
    ]
    """
    event_dict = [
        {
            "fc": {
                "reftime": reftime.date().isoformat(),
                "hc_year": int(hc_year),
                "number": int(number),
            },
            # "leadtime": int(leadtime),
            "leadtime": pd.Timedelta(leadtime).isoformat(),
        }
        for (reftime, hc_year, number), leadtime in events
    ]

    return event_dict


def eventdict_to_json(event_dict, path, short_path=False, split_reftimes=False):
    DEFAULT_DIR = (
        "/project/meteo/work/Jonas.Spaeth/Master-Thesis/processed-data/json/s2s_events/"
    )

    def save_dict(e_d, path):
        Path(path.rpartition("/")[0]).mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fout:
            json.dump(e_d, fout)

    if not split_reftimes:
        path = DEFAULT_DIR + path if short_path else path
        save_dict(event_dict, path)
    else:
        event_dict_splitted = events_split_reftime(event_dict)
        for ref in event_dict_splitted:
            ref_str = ref["reftime"]
            events_oneref = ref["events"]
            path_splitted = (
                DEFAULT_DIR
                + path.split(".json")[0]
                + "_ref{}.json".format(ref_str.replace("-", ""))
                if short_path
                else path.split(".json")[0]
                     + "_ref{}.json".format(ref_str.replace("-", ""))
            )
            save_dict(events_oneref, path_splitted)


def events_split_reftime(eventdict):
    """
    convert from
    event_dict = [{"fc": {"reftime": , "hc_year": , "number": }, "leadtime": }, {...}]
    to
    event_dict_splitted = [{"reftime": , "events": [{"fc": {"reftime": , "hc_year": , "number": }, "leadtime": }, {...}]}, {...}]
    """
    event_dict_splitted = []

    for e in eventdict:
        ref = e["fc"]["reftime"]
        idx = next(
            (
                index
                for (index, d) in enumerate(event_dict_splitted)
                if d.get("reftime") == ref
            ),
            None,
        )
        if idx != None:
            # reftime exists
            event_dict_splitted[idx]["events"].append(e)
        else:
            # reftime does not exist yet
            event_dict_splitted.append({"reftime": ref, "events": [e]})

    return event_dict_splitted


# %%

def eventlist_from_json(path):
    event_list = []
    for f in glob(path):
        with open(f) as infile:
            event_list = event_list + json.load(infile)
    return event_list


def composite_from_eventlist(event_list, data):
    event_comp = []
    missing_reftime_keys = []

    for event in tqdm(event_list, desc="Opening Events"):
        central_day = np.timedelta64(pd.Timedelta(event["leadtime"]))
        if np.datetime64(event["fc"]["reftime"]) in data.reftime.values:
            forecast = data.sel(
                reftime=event["fc"]["reftime"],
                hc_year=event["fc"]["hc_year"],
                number=event["fc"]["number"],
            )
            event_comp.append(
                forecast.assign_coords(
                    leadtime=data.leadtime - central_day
                ).rename(leadtime="lagtime")
            )
        elif event["fc"]["reftime"] in missing_reftime_keys:
            continue
        else:
            missing_reftime_keys.append(event["fc"]["reftime"])
            print(
                "KeyError Warning: reftime {} not in data".format(
                    event["fc"]["reftime"]
                )
            )

    event_comp = xr.concat(event_comp, dim="i")

    return event_comp.assign_coords(i=event_comp.i)


def composite_from_json(path, data):
    event_list = eventlist_from_json(path)
    composite = composite_from_eventlist(event_list, data)
    return composite


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
