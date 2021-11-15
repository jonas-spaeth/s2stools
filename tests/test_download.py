import unittest
import numpy as np
from download.ecmwf import S2SDownloaderECMWF


class TestDownload(unittest.TestCase):

    # ECMWF

    def test_filter_reftimes(self):
        dl = S2SDownloaderECMWF()
        dates = np.arange("2021-11-15", "2021-12-15", dtype="datetime64[D]")

        valid_dates = dl.filter_reftimes(dates)

        self.assertEqual(len(valid_dates), 9)
        self.assertIn(np.datetime64("2021-11-15"), valid_dates)
        self.assertNotIn(np.datetime64("2021-11-16"), valid_dates)

    def test_retreive(self):
        dl = S2SDownloaderECMWF()
        dates = np.arange("2020-11-15", "2020-12-20", dtype="datetime64[D]")

        target = "/Users/Jonas.Spaeth/Developer/stos/data/uv.nc"

        dl.retreive(
            param=["u", "v"],
            reftime=dates[:3],
            plevs=[1000, 850],
            step = [0, 24],
            target = target,
            grid="2.5/2.5"
        )


if __name__ == '__main__':
    unittest.main()
