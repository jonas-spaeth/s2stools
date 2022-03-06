from unittest import TestCase
import numpy as np
import pandas as pd

from s2stools.download.ecmwf import S2SDownloaderECMWF
from s2stools.download.ecmwf import model_setup


class TestS2SDownloaderECMWF(TestCase):

    def test_retreive(self):
        if False:  # set to true to test download via ECMWF API
            dl = S2SDownloaderECMWF()
            dates = np.arange("2020-11-15", "2020-12-20", dtype="datetime64[D]")

            dl.retreive(
                param=["u", "v"],
                file_descr="uv",
                reftime=dates[:5],
                plevs=[1000],
                step=[0, 24],
                path="../../data",
                grid="2.5/2.5",
                rt_cf_kwargs={},
                rt_pf_kwargs=dict(number="1/2/3"),
                # hc_cf_kwargs=dict(hdate="2000-11-16/2001-11-16"),
                hc_pf_kwargs=dict(number="1/2"),
                write_info_file=True
            )

    def test_retreive_rt_cf_only(self):
        if False:  # set to true to test download via ECMWF API
            dl = S2SDownloaderECMWF()
            dates = np.arange("2020-11-15", "2020-12-20", dtype="datetime64[D]")

            dl.retreive(
                param=["u", "v"],
                file_descr="uv",
                reftime=dates[:5],
                plevs=[1000],
                step=[0, 24],
                path="../../data",
                grid="2.5/2.5",
                rt_cf_kwargs={},
                rt_pf_kwargs=dict(number="1/2/3", skip=True),
                hc_cf_kwargs=dict(skip=True), #hdate="2000-11-16/2001-11-16"
                hc_pf_kwargs=dict(number="1/2", skip=True),
                write_info_file=True
            )


    def test_filter_reftimes(self):
        dl = S2SDownloaderECMWF()
        dates = np.arange("2021-11-15", "2021-12-15", dtype="datetime64[D]")

        valid_dates = dl.filter_reftimes(dates)

        self.assertEqual(len(valid_dates), 9)
        self.assertIn(np.datetime64("2021-11-15"), valid_dates)
        self.assertNotIn(np.datetime64("2021-11-16"), valid_dates)

    def test_hdates_all(self):
        leap_yr_jan = np.datetime64("2016-01-16")
        leap_yr_nov = np.datetime64("2016-11-16")
        non_leap_yr_jan = np.datetime64("2017-01-16")
        non_leap_yr_nov = np.datetime64("2017-11-16")
        leap_yr_29feb = np.datetime64("2016-02-29")

        test_dates = [leap_yr_jan, leap_yr_nov, non_leap_yr_jan, non_leap_yr_nov, leap_yr_29feb]
        for d in test_dates:
            d_pd = pd.to_datetime((d))
            client = model_setup.Hc(d)
            hdates = client.hdate_all(d)
            # days and months of hindcast dates are equal
            self.assertEqual(0, np.max(np.abs(np.diff(pd.to_datetime(hdates).day))), msg="1")
            self.assertEqual(0, np.max(np.abs(np.diff(pd.to_datetime(hdates).month))), msg="2")
            # months and days are equal to realtime date (except: 29 feb)
            if (d_pd.month == 2) & (d_pd.day == 29):
                self.assertEqual(pd.to_datetime(hdates[0]).month, 2)
                self.assertEqual(pd.to_datetime(hdates[0]).day, 28)
            else:
                self.assertEqual(pd.to_datetime(d).day, pd.to_datetime(hdates[0]).day, msg="3")
                self.assertEqual(pd.to_datetime(d).month, pd.to_datetime(hdates[0]).month, msg="4")
