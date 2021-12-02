from unittest import TestCase
from s2stools import process, utils
import os
import xarray as xr


class TestProcess(TestCase):

    def test_open_files(self):
        directory = "../../data/"
        ds = process.open_files(path_pattern=directory + "s2s_ecmwf_uv_20*", max_lt=1)
        self.assertIsInstance(ds, xr.Dataset)

    def test_add_validtime(self):
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        print(ds)
        ds_vt = process.add_validtime(ds)
        self.assertIn("validtime", list(ds_vt.coords))
        vt_min = ds_vt.validtime.min()
        hc_year_min = ds.hc_year.min().values
        reftime_min = ds.reftime.min().values
        earliest_date = utils.add_years(reftime_min, hc_year_min)
        self.assertEqual(earliest_date, vt_min)
