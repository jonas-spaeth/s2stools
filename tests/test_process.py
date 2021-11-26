from unittest import TestCase
import process
import os
import xarray as xr


class TestProcess(TestCase):

    def test_open_files(self):
        directory = "../data/"
        print(os.listdir("../data"))
        ds = process.open_files(path_pattern=directory + "s2s_ecmwf_uv_20*", max_lt=1)
        self.assertIsInstance(ds, xr.Dataset)
