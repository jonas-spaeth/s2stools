import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar


def save_one_file_per_reftime(data: xr.Dataset, path: str):
    """
    Save S2S Dataset with one file per reftime.
    Args:
        data: (xr.Dataset)
        path: (str) target path including filename. _REFTIME.nc will be added. E.g.: /home/foo/s2s_somefilename

    Returns:
        void
    """
    reftimes, datasets = zip(*data.groupby("reftime"))
    datasets = [d.expand_dims("reftime") for d in datasets]
    paths = [
        f"{path}_{f}.nc"
        for f in pd.DatetimeIndex(reftimes).strftime("%Y-%m-%d")
    ]
    result = xr.save_mfdataset(datasets, paths, compute=False)

    with ProgressBar():
        result.compute()
