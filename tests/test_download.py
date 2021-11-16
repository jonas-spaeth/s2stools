from unittest import TestCase
import numpy as np
from download.ecmwf import S2SDownloaderECMWF
import xarray as xr
import matplotlib.pyplot as plt


class TestS2SDownloaderECMWF(TestCase):

    def test_retreive(self):
        dl = S2SDownloaderECMWF()
        dates = np.arange("2020-11-15", "2020-12-20", dtype="datetime64[D]")

        dl.retreive(
            param=["u", "v"],
            file_descr="uv",
            reftime=dates[:3],
            plevs=[1000],
            step=[0, 24],
            path="../data",
            grid="2.5/2.5",
            rt_cf_kwargs={},
            rt_pf_kwargs=dict(number="1/2/3"),
            hc_cf_kwargs=dict(hdate="2000-11-16/2001-11-16"),
            hc_pf_kwargs=dict(hdate="2000-11-16/2001-11-16", number="1/2"),
            write_info_file=False
        )

    def test_filter_reftimes(self):
        dl = S2SDownloaderECMWF()
        dates = np.arange("2021-11-15", "2021-12-15", dtype="datetime64[D]")

        valid_dates = dl.filter_reftimes(dates)

        self.assertEqual(len(valid_dates), 9)
        self.assertIn(np.datetime64("2021-11-15"), valid_dates)
        self.assertNotIn(np.datetime64("2021-11-16"), valid_dates)
