from unittest import TestCase
from s2stools import clim, process


class TestClim(TestCase):
    
    def test_deseasonalize(self):
        data = process.open_files(path_pattern="../../data/s2s_*")
        # absolute anomalies
        anom = clim.deseasonalize(data, standardize=False)
        for var in list(anom.keys()):
            self.assertAlmostEqual(anom[var].mean().values, 0, delta=0.1)

        # standardized absolute anomalies
        anom = clim.deseasonalize(data, standardize=True)
        for var in list(anom.keys()):
            self.assertAlmostEqual(anom[var].mean().values, 0, delta=0.1)
            self.assertAlmostEqual(anom[var].std().values, 1, delta=0.1)



