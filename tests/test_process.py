import numpy as np
import xarray as xr
from s2stools.process import (
    s2sparser,
    add_model_cycle_ecmwf,
    add_validtime,
    concat_era5_before_s2s,
    stack_fc,
    stack_ensfc,
    reft_hc_year_to_fc_init_date,
    combine_s2s_and_reanalysis,
    _infer_reftime_from_filename,
    sel_fc_around_dates,
    table_of_fc_around_dates,
)
from tests.utils import DATA_PATH


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
    ds = open_files_test(f"{DATA_PATH}/s2s*.nc")
    # test with 2 reftimes, with only realtime forecasts
    _ = open_files_test(f"{DATA_PATH}/s2s*f_*.nc")
    # test with 2 reftimes, with only hindcast forecasts
    _ = open_files_test(f"{DATA_PATH}/s2s*hc_*.nc")
    # test with 1 reftime
    _ = open_files_test(f"{DATA_PATH}/s2s*20171116*.nc")


def test_add_model_cycle_ecmwf():
    ds_raw = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    ds = add_model_cycle_ecmwf(ds_raw)
    assert "cycle" in ds.coords


def test_add_validtime():
    ds_raw = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    assert "validtime" in ds_raw.coords
    ds_wo_vt = ds_raw.drop_vars("validtime")
    assert "validtime" not in ds_wo_vt.coords
    ds_new = add_validtime(ds_wo_vt)
    assert "validtime" in ds_new.coords


def test_concat_era5_before_s2s():
    # open forecast and reanalysis dataset
    ds_s2s = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    ds_era5 = xr.open_mfdataset(f"{DATA_PATH}/era5*.nc")

    ds_combined = concat_era5_before_s2s(ds_s2s.u, ds_era5.u, max_neg_leadtime_days=10)
    print(ds_combined)


def test_stack_fc():
    ds = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    ds_stacked = stack_fc(ds)
    assert "fc" in ds_stacked.dims


def test_stack_ensfc():
    ds = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    ds_stacked = stack_ensfc(ds)
    assert "fc" in ds_stacked.dims


def test_reft_hc_year_to_fc_init_date():
    ds = xr.open_mfdataset(f"{DATA_PATH}/s2s*hc_*.nc", preprocess=s2sparser)
    ds_fc_init_date = reft_hc_year_to_fc_init_date(ds)
    assert "fc_init_date" in ds_fc_init_date.dims
    # check if #hc_years * #reftimes = #fc_init_dates
    N_hc = ds.hc_year.size
    N_reft = ds.reftime.size
    N_fc_init_dates = ds_fc_init_date.fc_init_date.size
    assert N_hc * N_reft == N_fc_init_dates


def test_combine_s2s_and_reanalysis():
    # open forecast and reanalysis dataset
    ds_s2s = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)
    ds_era5 = xr.open_mfdataset(f"{DATA_PATH}/era5*.nc")
    print(ds_s2s)
    print(ds_era5)
    # DataArray
    ds_combined = combine_s2s_and_reanalysis(ds_s2s.u, ds_era5.u)
    # Dataset
    ds_combined = combine_s2s_and_reanalysis(ds_s2s, ds_era5)
    # Dataset with different names
    ds_combined = combine_s2s_and_reanalysis(ds_s2s, ds_era5.rename(u="u_reanalysis"))
    # Dataset without stacking to ensfc
    ds_combined = combine_s2s_and_reanalysis(ds_s2s, ds_era5, ensfc=False)
    print(ds_combined)


def test__infer_reftime_from_filename():
    # works
    path = f"{DATA_PATH}/s2s_u60_10hPa_20171116_cf_short.nc"
    result = _infer_reftime_from_filename(path)
    assert isinstance(result, np.datetime64)

    # works
    path = f"{DATA_PATH}/s2s_u60_10hPa_cf_2017_11_16_short.nc"
    result = _infer_reftime_from_filename(path)
    assert isinstance(result, np.datetime64)

    # does not work
    path = f"{DATA_PATH}/s2s_u60_10hPa_cf_short.nc"
    result = _infer_reftime_from_filename(path)
    assert not isinstance(result, np.datetime64)

    # works
    path = f"{DATA_PATH}/s2s_something_else_2017-11-01.nc"
    result = _infer_reftime_from_filename(path)
    assert isinstance(result, np.datetime64)

    # works
    path = f"{DATA_PATH}/s2s_something_else_20171101.nc"
    result = _infer_reftime_from_filename(path)
    assert isinstance(result, np.datetime64)


def test_sel_fc_around_dates():
    ds = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)

    # test with dates that are available
    dates = np.array(["2015-11-16", "2011-11-17"], dtype="datetime64")
    selected_fc = sel_fc_around_dates(ds, dates, tolerance_days=3)
    # assert selected_fc.fc_start.values == np.array(["2015-11-16", "2011-11-16", "2011-11-20"], dtype="datetime64")
    np.testing.assert_array_equal(
        selected_fc.fc_start.values,
        np.array(["2015-11-16", "2011-11-16", "2011-11-20"], dtype="datetime64"),
    )

    # test with one available and one not available date
    dates = np.array(["2024-11-16", "2011-11-17"], dtype="datetime64")
    selected_fc = sel_fc_around_dates(ds, dates, tolerance_days=1)
    np.testing.assert_array_equal(
        selected_fc.fc_start.values, np.array(["2011-11-16"], dtype="datetime64")
    )

    # test with dates that are not available
    dates = np.array(["2024-11-16", "2024-11-17"], dtype="datetime64")
    selected_fc = sel_fc_around_dates(ds, dates, tolerance_days=1)
    assert selected_fc is None


def test_table_of_fc_around_dates():
    ds = xr.open_mfdataset(f"{DATA_PATH}/s2s*.nc", preprocess=s2sparser)

    dates = np.array(["2015-11-16", "2011-11-17"], dtype="datetime64")
    table = table_of_fc_around_dates(ds, dates, tolerance_days=3)
    assert table.shape == (3, 2)
