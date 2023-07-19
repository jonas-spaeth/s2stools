import copy
import json
from datetime import datetime
from glob import glob
from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
from tqdm.autonotebook import tqdm


class EventComposite:
    # attributes
    data = None
    """
    Obtain the original feded data using my_event_composite.data
    """
    comp = None
    """
    Access the composite dataset using my_event_composite.comp
    """
    descr = ""
    """
    Access the composite description using my_event_composite.descr
    """
    event_list_json = []

    # init
    def __init__(self, data, events, descr, model):
        """
        Composite class

        Generate an instance my_composite of the class `EventComposite` and
        analyse the composite data using instance my_composite.comp.

        Parameters
        ----------
        data : xr.Dataset
            dataset with appropriate dimensions (reftime, leadtime, hc_year, number, ...)
        events : (str or list)
            if str, then define the path, e.g., json/s2s_events/MODEL/ssw/*
            (model will automatically replaced by self.model)
            if list then of format [{fc: {reftime: None, hc_year: None, number: None}, days_since_init: None}, {...}]
        descr : str
            event description, e.g., used for plot titles
        model : str
            model name, e.g., ecmwf or ukmo


        Examples
        --------
        >>> ssw_composite = EventComposite(data, "path/to/eventlists*.json", descr="sudden warmings", model="ecmwf")
        >>> ssw_composite
        <s2stools.events.EventComposite> of 182 events of type 'sudden warmings'.
            --> see composite dataset using my_event_composite.comp
        >>> ssw_composite.comp
        <xarray.Dataset>
        Dimensions:    (lagtime: 73, longitude: 2, latitude: 1, i: 182)
        Coordinates:
          * lagtime    (lagtime) timedelta64[ns] -36 days -35 days ... 35 days 36 days
          * longitude  (longitude) float32 -180.0 -177.5
          * latitude   (latitude) float32 60.0
            number     (i) int64 0 3 4 5 6 8 10 3 8 10 ... 39 40 42 43 44 45 46 47 48 49
            reftime    (i) datetime64[ns] 2017-11-16 2017-11-16 ... 2017-11-20
            hc_year    (i) int64 -20 -20 -20 -20 -20 -20 -20 -19 -19 ... 0 0 0 0 0 0 0 0
            validtime  (i, lagtime) datetime64[ns] NaT NaT NaT NaT ... NaT NaT NaT NaT
            leadtime   (i) timedelta64[ns] 11 days 11 days 10 days ... 19 days 16 days
          * i          (i) int64 0 1 2 3 4 5 6 7 8 ... 174 175 176 177 178 179 180 181
        Data variables:
            u          (i, latitude, longitude, lagtime) float32 dask.array<chunksize=(1, 1, 2, 73), meta=np.ndarray>
        """
        self.descr = descr  # something
        self.model = model
        # events from json
        if isinstance(events, str):
            # assert that event_jsons_path describes a path
            self.event_list_json = _eventlist_from_json(
                events.replace("MODEL", model)
            )
        elif isinstance(events, list):
            # assert event_jsons_path is the list of events
            self.event_list_json = events
        else:
            raise TypeError
        # assert that data has key leadtime, not days_since_init
        self.data = _dataset_ensure_leadtime_key(data)
        # assert that event_list has key leadtime, not days_since_init
        self.event_list_json = _eventlist_ensure_leadtime_key(self.event_list_json)
        # create composite from event list
        self.comp = _composite_from_eventlist(self.event_list_json, self.data)

    def __len__(self):
        return len(self.comp.i)

    def __repr__(self):
        return f"<s2stools.events.EventComposite> of {len(self)} events of type '{self.descr}'\n" \
               f"\t--> see composite dataset using my_event_composite.comp"


def _eventlist_from_json(path):
    event_list = []
    for f in glob(path):
        with open(f) as infile:
            event_list = event_list + json.load(infile)
    return event_list


def _eventlist_ensure_leadtime_key(eventlist):
    coords = list(list(eventlist[0].keys()))
    if "leadtime" in coords:
        return eventlist
    elif "days_since_init" in coords:
        return _format_eventlist_dayssinceinit_to_pdtimedeltastamp(eventlist)
    else:
        raise ValueError(
            "eventlist must have one of ('leadtime', 'days_since_init') as coordinate. (Has: {})".format(coords))


def _format_eventlist_dayssinceinit_to_pdtimedeltastamp(eventlist):
    new_eventlist = copy.deepcopy(eventlist)
    for e in new_eventlist:
        e["leadtime"] = e.pop("days_since_init")
        e["leadtime"] = pd.Timedelta(e["leadtime"], "D").isoformat()
    return new_eventlist


def _dataset_ensure_leadtime_key(dataset):
    coords = list(dataset.coords)
    if "leadtime" in coords:
        return dataset
    elif "days_since_init" in coords:
        print("Warning: .data will have coordinate 'leadtime', not 'days_since_init'. Hope that's okay?")
        return dataset.assign_coords(
            days_since_init=pd.TimedeltaIndex(dataset.days_since_init.values, "D")
        ).rename(days_since_init="leadtime")
    else:
        raise ValueError(
            "dataset must have one of ('leadtime', 'days_since_init') as coordinate. (Has: {})".format(coords))


def _composite_from_eventlist(event_list, data):
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
                ).rename(leadtime="lagtime").assign_coords(leadtime=central_day)
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
    :func:`s2stools.events._composite_from_eventlist`

    Warnings
    --------
    There could have been a major SSW before the start of the forecast.
    In that case, the first zero-crossing in the forecast might actually not be a real SSW.
    In the future, it would be nice to append the reanalysis ahead of the forecast to check the preceding
    vortex evolution.
    """
    buffer_start, buffer_end, require_westwind_start = map(_to_timedelta64,
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

    event_dict = _eventlist_to_dict(events)

    return event_dict


def _eventlist_to_dict(events):
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
            "leadtime": pd.Timedelta(leadtime).isoformat(),
        }
        for (reftime, hc_year, number), leadtime in events
    ]

    return event_dict


def _eventdict_to_json(event_dict, path, short_path=False, split_reftimes=False):
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
        event_dict_splitted = _events_split_reftime(event_dict)
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


def _events_split_reftime(eventdict):
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


def _to_timedelta64(a, assume="D"):
    """
    Assume
    Args:
        a (int or np.timedelta64): timedelta
        assume (): timedelta64 format that is assumed if a is of type int.

    Returns:
        np.timedelta64
    """
    if isinstance(a, int):
        a = np.timedelta64(a, assume)
    return a


def ssw_compendium_event_dates(column='ERA-Interim'):
    """
    Read html table from SSW compendium

    Butler et al. 2017 A sudden stratospheric warming compendium https://doi.org/10.5194/essd-9-63-2017

    Parameters
    ----------
    column : str
        Indicating the different datasets, see website for available options. Defaults to 'ERA-Interim'.

    Returns
    -------
    dates : pd.Series
        Sudden Warming dates

    """

    result = pd.read_html(
        "https://csl.noaa.gov/groups/csl8/sswcompendium/majorevents.html"
    )
    df_raw = result[0]

    # parse dates
    def parse_dates(x):
        if "-" in str(x):
            x = pd.to_datetime(str(x), format="%d-%b-%y")
            x = x.replace(year=x.year - 100) if x.year > datetime.now().year else x
            return x
        else:
            return x

    df = df_raw.applymap(parse_dates)

    # filter for valid dates
    dates = df[column][
        df[column].apply(lambda x: isinstance(x, pd.Timestamp))
    ]

    return dates
