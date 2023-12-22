import pytest
import xarray as xr
from s2stools.process import s2sparser
from s2stools.events import EventComposite, find_ssw, find_events
from tests.utils import *


def test_event_composite():
    ds = xr.open_mfdataset(f"{DATA_PATH}/*.nc", preprocess=s2sparser)
    # create composite using path to files
    ssw_composite = EventComposite(
        ds, f"{DATA_PATH}/ssw*", descr="sudden warmings", model="ecmwf"
    )
    assert "i" in ssw_composite.comp.dims

    # create composite using list of events
    ssws = find_ssw(ds.mean("longitude").u.load())
    ssw_composite2 = EventComposite(ds, ssws, descr="sudden warmings", model="ecmwf")
    assert isinstance(ssw_composite2.comp, xr.Dataset)


def test_find_ssws():
    # sudden stratospheric warmings
    ds = xr.open_mfdataset(f"{DATA_PATH}/*.nc", preprocess=s2sparser)
    ssws = find_ssw(ds.u.mean("longitude").load())


def test_find_events():
    ds = xr.open_mfdataset(f"{DATA_PATH}/*.nc", preprocess=s2sparser)
    u = ds.u.mean("longitude").load()
    min_event_length = 5
    min_days_between_events = 10
    event_start_dates, event_end_dates = find_events(
        u,
        min_event_length=min_event_length,
        min_days_between_events=min_days_between_events,
    )
    print(event_start_dates, event_end_dates)
    # as many start dates as end dates
    assert len(event_start_dates) == len(event_end_dates)
    # difference between start and end dates is at least min_event_length
    assert (event_end_dates - event_start_dates > min_event_length).sum() == 0
