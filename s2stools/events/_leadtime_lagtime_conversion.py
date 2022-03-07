import copy
import pandas as pd


def format_eventlist_dayssinceinit_to_pdtimedeltastamp(eventlist):
    new_eventlist = copy.deepcopy(eventlist)
    for e in new_eventlist:
        e["leadtime"] = e.pop("days_since_init")
        e["leadtime"] = pd.Timedelta(e["leadtime"], "D").isoformat()
    return new_eventlist


def dataset_ensure_leadtime_key(dataset):
    coords = list(dataset.coords)
    if "leadtime" in coords:
        return dataset
    elif "days_since_init" in coords:
        print("Warning: .data will have coordinate 'leadtime', not 'days_since_init'.")
        return dataset.assign_coords(
            days_since_init=pd.TimedeltaIndex(dataset.days_since_init.values, "D")
        ).rename(days_since_init="leadtime")
    else:
        raise ValueError(
            "dataset must have one of ('leadtime', 'days_since_init') as coordinate. (Has: {})".format(coords))


def eventlist_ensure_leadtime_key(eventlist):
    coords = list(list(eventlist[0].keys()))
    if "leadtime" in coords:
        return eventlist
    elif "days_since_init" in coords:
        return format_eventlist_dayssinceinit_to_pdtimedeltastamp(eventlist)
    else:
        raise ValueError(
            "eventlist must have one of ('leadtime', 'days_since_init') as coordinate. (Has: {})".format(coords))
