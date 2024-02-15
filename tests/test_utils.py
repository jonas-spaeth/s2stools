import pytest

import numpy as np
import xarray as xr
import pandas as pd
from s2stools.utils import wrap_time, unwrap_time, wrap_time2, unwrap_time2


def _dummy_da_with_times(times):
    nt = len(times)
    return xr.DataArray(
        np.random.randint(low=0, high=100, size=(nt, 10)),
        coords=dict(time=times, x=np.arange(10)),
        dims=["time", "x"],
    )


def test_wrap_time():
    # first, test with change_dims=False

    # continuous days
    times = pd.date_range("2000-11-03", "2003-12-31", freq="D")
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=11, change_dims=False
    )
    assert result.dayofseason[0] == 3

    # daily, but only djf data
    times = pd.date_range("2000-12-02", "2003-12-31", freq="D")
    times = times[np.isin(times.month, [12, 1, 2])]
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=12, change_dims=False
    )
    assert result.dayofseason[0] == 2

    # daily, only djf data, but start with non-leap year
    times = pd.date_range("2001-12-02", "2003-12-31", freq="D")
    times = times[np.isin(times.month, [12, 1, 2])]
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=12, change_dims=False
    )
    assert result.dayofseason[0] == 2

    # second, test with change_dims=True

    # leap year
    times = pd.date_range("2000-10-02", "2000-10-04", freq="D")
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=10, change_dims=True
    )
    assert result.dayofseason[0] == 2

    # non-leap year
    times = pd.date_range("2001-12-02", "2001-12-04", freq="D")
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=12, change_dims=True
    )
    assert result.dayofseason[0] == 2

    # does it work to start with january?
    times = pd.date_range("2000-01-01", "2008-12-31", freq="D")
    result = wrap_time(
        _dummy_da_with_times(times), season_start_month=12, change_dims=True
    )


def test_unwrap_time():
    times = pd.date_range("2000-11-01", "2001-12-01", freq="D")
    result1 = wrap_time(
        _dummy_da_with_times(times), season_start_month=11, change_dims=True
    )
    print(result1)
    result2 = unwrap_time(result1)
    print(result2)


def test_wrap_time2():
    times = pd.date_range("2000-11-01", "2001-12-01", freq="D")
    result = wrap_time2(
        _dummy_da_with_times(times), season_start_month=11, change_dims=False
    )
    print(result)


def test_unwrap_time2():
    times = pd.date_range("2000-11-01", "2008-12-01", freq="D")
    ds = _dummy_da_with_times(times)

    result1 = wrap_time2(ds, season_start_month=11, change_dims=False)

    result2 = unwrap_time2(result1)

    assert (ds - result2).max().values == 0
