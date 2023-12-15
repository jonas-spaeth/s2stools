import pytest
import xarray as xr
from s2stools.process import s2sparser
from s2stools.events import EventComposite, find_ssw
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


def test_find_events():
    # sudden stratospheric warmings
    ds = xr.open_mfdataset(f"{DATA_PATH}/*.nc", preprocess=s2sparser)
    ssws = find_ssw(ds.u.mean("longitude").load())
