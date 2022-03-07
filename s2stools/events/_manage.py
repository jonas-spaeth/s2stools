from tqdm import tqdm
import numpy as np
import pandas as pd
import xarray as xr
import json
from glob import glob
import copy


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


def eventlist_from_json(path):
    """Read a bunch of json files and generate + return an event list.

    Args:
        path (str): directory of json files, supports path globbing

    Returns:
        list: event list of format [{"fc": {"reftime": None, "hc_year": None, "number": None}, "days_since_init": None}, {...}]
    """
    event_list = []
    for f in glob(path):
        with open(f) as infile:
            event_list = event_list + json.load(infile)
    return event_list


def rename_eventlist_key(eventlist, mapping):
    """

    Args:
        mapping (dict): {"<oldkey>": "<newkey>"}

    Returns:
        updated eventlist
    """
    new_eventlist = copy.deepcopy(eventlist)
    for e in new_eventlist:
        for oldname, newname in mapping.items():
            if oldname in e:
                e[newname] = e.pop(oldname)
    return new_eventlist
