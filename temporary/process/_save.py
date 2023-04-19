import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from pathlib import Path

def save_one_file_per_reftime(data: xr.Dataset, path: str, create_subdirectory=None):
    """
    Save S2S Dataset with one file per reftime.
    Args:
        data: xr.Dataset
            Data
        path: str
            target path including filename. _REFTIME.nc will be added. E.g.: /home/foo/s2s_somefilename

    Parameters
    ----------
        data: xr.Dataset
            Dataset to save.
        path: str
            target path including filename. _REFTIME.nc will be added. E.g.: /home/foo/s2s_somefilename
        create_subdirectory: str or None
            Check if the subdirectory exists. If yes, raise error. If no, create subdirectory and save files into this subdirectory. Defaults to None, where no subdirectory is created and the files are just saved to 'path'.


    """
    if create_subdirectory is not None:
        # e.g. if path = /foo/filename
        dir_excl_subdir = str.join("/", path.split("/")[:-1])  # e.g.: /foo
        dir_incl_subdir = dir_excl_subdir + "/" + create_subdirectory # e.g.: /foo/bar
        Path(dir_incl_subdir).mkdir(parents=True, exist_ok=False)
        path = dir_incl_subdir + "/" + path.split("/")[-1] # e.g.: /foo/bar/filename

    reftimes, datasets = zip(*data.groupby("reftime", squeeze=False))
    #print(datasets[0])
    # datasets = [d.expand_dims("reftime") for d in datasets]
    paths = [
        f"{path}_{f}.nc"
        for f in pd.DatetimeIndex(reftimes).strftime("%Y-%m-%d")
    ]
    result = xr.save_mfdataset(datasets, paths, compute=False)

    with ProgressBar():
        result.compute()
