import xarray as xr
from s2stools.process import s2sparser


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
    ds = open_files_test("data/*.nc")
    # test with 2 reftimes, with only realtime forecasts
    _ = open_files_test("data/*f_*.nc")
    # test with 2 reftimes, with only hindcast forecasts
    _ = open_files_test("data/*hc_*.nc")
    # test with 1 reftime
    _ = open_files_test("data/*20171116*.nc")
