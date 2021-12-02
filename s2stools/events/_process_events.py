import json
import pandas as pd
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


def eventlist_from_json(path):
    event_list = []
    for f in glob(path):
        with open(f) as infile:
            event_list = event_list + json.load(infile)
    return event_list
