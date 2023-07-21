from warnings import warn

import numpy as np
import xarray as xr
from s2stools.process import s2sparser, add_model_cycle_ecmwf, add_validtime, concat_era5_before_s2s


def test_s2sparser():
    """
    test s2sparser
    """
    expected_dims = ["reftime", "hc_year", "number"]

    def open_files_test(path):
        ds = xr.open_mfdataset(path, preprocess=s2sparser)
        for ed in expected_dims:
            assert ed in ds.dims
        return ds

    # test with 2 reftimes, with cf, pf, chc, phc
    ds = open_files_test("data/s2s*.nc")
    # test with 2 reftimes, with only realtime forecasts
    _ = open_files_test("data/s2s*f_*.nc")
    # test with 2 reftimes, with only hindcast forecasts
    _ = open_files_test("data/s2s*hc_*.nc")
    # test with 1 reftime
    _ = open_files_test("data/s2s*20171116*.nc")


def test_add_model_cycle_ecmwf():
    ds_raw = xr.open_mfdataset('data/s2s*.nc', preprocess=s2sparser)
    ds = add_model_cycle_ecmwf(ds_raw)
    assert 'cycle' in ds.coords


def test_add_validtime():
    ds_raw = xr.open_mfdataset('data/s2s*.nc', preprocess=s2sparser)
    assert 'validtime' in ds_raw.coords
    ds_wo_vt = ds_raw.drop_vars('validtime')
    assert 'validtime' not in ds_wo_vt.coords
    ds_new = add_validtime(ds_wo_vt)
    assert 'validtime' in ds_new.coords


def test_concat_era5_before_s2s():
    # open forecast and reanalysis dataset
    ds_s2s = xr.open_mfdataset('data/s2s*.nc', preprocess=s2sparser)
    ds_era5 = xr.open_mfdataset('data/era5*.nc')

    # memory warning if dataset has dimension number
    if 'number' in ds_s2s.dims:
        if len(ds_s2s.number) > 1:
            warn(
                'If dimension number in S2S dataset, then padding ERA5 before forecast start leads to considerable' \
                'memory usage, as all ensemble members are padded with the same values.',
                ResourceWarning
            )

    # reindex s2s forecasts to negative lags
    max_lt = ds_s2s.isel(leadtime=-1).leadtime.values.astype('timedelta64[D]').astype('int')
    max_neg_leadtime_days = 10
    ds_s2s_neg_lag_idx = add_validtime(
        ds_s2s.drop_vars('validtime').reindex(
            leadtime=np.arange(-max_neg_leadtime_days, max_lt + 1, dtype='timedelta64[D]')
        )
    )

    # reindex era5 to available s2s valid dates
    validtime_flattened_dates = np.unique(ds_s2s_neg_lag_idx.validtime.values.flatten())
    ds_era5_padded = ds_era5.reindex(time=validtime_flattened_dates)

    # project reanalysis on s2s
    ds_combined = concat_era5_before_s2s(ds_s2s.u, ds_era5_padded.u, max_neg_leadtime_days=max_neg_leadtime_days)
