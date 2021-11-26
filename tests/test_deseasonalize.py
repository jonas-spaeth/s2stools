from unittest import TestCase
import xarray as xr
import deseasonalize as des
import process
import matplotlib.pyplot as plt

class TestDeseasonalize(TestCase):
    
    def test_deseasonalize(self):
        data = process.open_files(path_pattern="../data/s2s_*")
        # absolute anomalies
        anom = des.deseasonalize(data, standardize=False)
        for var in list(anom.keys()):
            self.assertAlmostEqual(anom[var].mean().values, 0, delta=0.1)

        # standardized absolute anomalies
        anom = des.deseasonalize(data, standardize=True)
        for var in list(anom.keys()):
            self.assertAlmostEqual(anom[var].mean().values, 0, delta=0.1)
            self.assertAlmostEqual(anom[var].std().values, 1, delta=0.1)



