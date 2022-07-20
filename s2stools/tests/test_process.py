import glob
import tempfile
from unittest import TestCase

import numpy as np
from matplotlib import pyplot as plt

from s2stools import process, utils
import os
import xarray as xr


class TestProcess(TestCase):

    def test_open_files(self):
        directory = "../../data/"
        ds = process.open_files(path_pattern=directory + "s2s_ecmwf_uv_20*")
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

    def test_zonal_wavenumber_decomposition(self):
        # create some test data
        lons = np.arange(360)
        lats = np.arange(5)
        nlon, nlat = len(lons), len(lats)
        k1 = 1 * np.sin(np.deg2rad(lons))
        k2 = 2 * np.sin(2 * np.deg2rad(lons))
        k4 = 4 * np.sin(4 * np.deg2rad(lons))
        k5 = 5 * np.sin(5 * np.deg2rad(lons))

        mean = 3

        # create a sample data with zonal wavestructures and latitudinal scaling from 0 to 1
        data = xr.DataArray(mean + np.outer(k1 + k2 + k4 + k5, np.linspace(0, 1, nlat)), coords={'longitude': lons, 'latitude': lats}, dims=['longitude', 'latitude'])

        # data.plot()
        # plt.show()

        data_k = process.zonal_wavenumber_decomposition(data.chunk(chunks=dict(latitude=nlat)))

        #        data_k.plot(hue='k', marker='o')
        #        plt.grid()
        #        plt.show()

        # check if amplitudes can be recovered
        self.assertAlmostEqual(data_k.sel(k='0').max().values, np.abs(mean))
        self.assertAlmostEqual(data_k.sel(k='1').max().values, 1)
        self.assertAlmostEqual(data_k.sel(k='2').max().values, 2)
        self.assertAlmostEqual(data_k.sel(k='4-7').max().values, 4 + 5)

    def test_save_one_file_per_reftime(self):
        directory = "../../data/"
        ds = process.open_files(path_pattern=directory + "s2s_ecmwf_uv_20*").isel(longitude=0, latitude=0)
        ds = ds.load()
        with tempfile.TemporaryDirectory() as dirpath:
            # temporary directory to save files
            process.save_one_file_per_reftime(ds, f"{dirpath}/testfile")
            directory = f'{dirpath}/testfile*'
            loaded = xr.open_mfdataset(directory)
            self.assertIsInstance(loaded, xr.Dataset)
            self.assertEqual(len(glob.glob(directory)), len(ds.reftime))
