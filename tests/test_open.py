from unittest import TestCase
import open
import os
import xarray as xr


class TestOpen(TestCase):

    def test_process_s2s(self):
        directory = "../data/"
        ds = open.process_s2s(path_pattern=directory+"s2s_ecmwf_uv_20*", max_lt=1)
        print(ds)
        self.assertIsInstance(ds, xr.Dataset)