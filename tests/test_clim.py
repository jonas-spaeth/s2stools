import pytest

import numpy as np
import xarray as xr
import pandas as pd
from s2stools.clim import climatology


def create_dummy_fc_dataarray():
    leadtimes = pd.timedelta_range("0 D", "46 D", freq="1D")
    hc_years = np.arange(-20, 1)
    numbers = np.arange(11)
    reftimes = pd.to_datetime(["2020-01-01", "2020-01-04"])
    lats = np.arange(0, 91, 10)
    n_lt, n_hcy, n_n, n_ref, n_lat = (
        len(leadtimes),
        len(hc_years),
        len(numbers),
        len(reftimes),
        len(lats),
    )
    data = np.random.normal(size=(n_ref, n_hcy, n_n, n_lt, n_lat))
    data = np.cumsum(np.abs(data), axis=-2)  # cumsum over leadtime
    da = xr.DataArray(
        data,
        coords=dict(
            leadtime=leadtimes,
            hc_year=hc_years,
            number=numbers,
            reftime=reftimes,
            latitude=lats,
        ),
        dims=["reftime", "hc_year", "number", "leadtime", "latitude"],
    )
    return da


def test_clim():
    da = create_dummy_fc_dataarray()
    da_clim = climatology(da)
    da_anom = da - da_clim
    # assert that anomalies are small compared to original data
    assert da_anom.mean() < da.mean() * 0.01

    # test different parameter settings
    _ = climatology(da, window_size=1, ndays_clim_filter=1)
    _ = climatology(da, mean_or_std="std")

    # test climatology if dim number not existing
    _ = climatology(da.mean("number"))

    # test with and without flox:
    with xr.set_options(use_flox=False):
        _ = climatology(da)
    with xr.set_options(use_flox=True):
        _ = climatology(da)

    # todo: implement and test climatology for groupby="validtime
    pass
