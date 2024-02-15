import pytest

import numpy as np
import xarray as xr
import pandas as pd
from s2stools.utils import wrap_time, unwrap_time


def _dummy_da_with_times(times):
    nt = len(times)
    return xr.DataArray(
        np.random.randint(low=0, high=100, size=(nt, 10)),
        coords=dict(time=times, x=np.arange(10)),
        dims=["time", "x"],
    )


def test_wrap_time():
    times = pd.date_range("2000-11-01", "2001-12-01", freq="D")
    ds = _dummy_da_with_times(times)
    _ = wrap_time(ds, season_start_month=11, change_dims=False)

    _ = wrap_time(_dummy_da_with_times(times), season_start_month=11, change_dims=True)


def test_unwrap_time():
    times = pd.date_range("2000-11-01", "2008-12-01", freq="D")
    ds = _dummy_da_with_times(times)

    result1 = wrap_time(ds, season_start_month=11, change_dims=False)

    result2 = unwrap_time(result1)

    # check that original data and wrapped-unwrapped data are the same
    assert (ds - result2).max().values == 0
