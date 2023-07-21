import numpy as np
import pandas as pd
import xarray as xr
import pandas as pd
from s2stools.utils import add_years
from pathlib import Path
from warnings import warn

IMPL_DATE = "ImplementationDate in S2S"


def s2sparser(ds):
    """
    Will create dimensions reftime, hc_year, leadtime.
    Coordinate validtime is automatically added.
    Files need to have the forecast realtime date somewhere in the filename, e.g., `s2s_something_2017-11-16.nc`.

    Parameters
    ----------
    ds : xr.Dataset
        dataset
    Returns
    -------
    xr.Dataset

    Warnings
    --------
    Realtime and hindcast forecasts are combined in a single dataset.
    If they have different ensemble sizes, then the resulting dataset is larger than necessary as coordinates span
    full dimension space, e.g., ensemble members 12-51 are padded with NaN.
    For a more efficient solution consider using xarray-datatree.

    Examples
    --------
    >>> # Use in the following form:
    >>> ds = xr.open_mfdataset("/some/path/filename_2017*.nc", preprocess=s2stools.process.s2sparser)
    >>> ds
    <xarray.Dataset>
    Dimensions:    (leadtime: 47, longitude: 2, latitude: 1, number: 51,
                    reftime: 2, hc_year: 21)
    Coordinates:
      * leadtime   (leadtime) timedelta64[ns] 0 days 1 days ... 45 days 46 days
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
      * number     (number) int64 0 1 2 3 4 5 6 7 8 9 ... 42 43 44 45 46 47 48 49 50
      * reftime    (reftime) datetime64[ns] 2017-11-16 2017-11-20
      * hc_year    (hc_year) int64 -20 -19 -18 -17 -16 -15 -14 ... -5 -4 -3 -2 -1 0
        validtime  (reftime, leadtime, hc_year) datetime64[ns] 1997-11-16 ... 201...
    Data variables:
        u          (reftime, latitude, longitude, leadtime, hc_year, number) float32 dask.array<chunksize=(1, 1, 2, 47, 20, 1), meta=np.ndarray>
    """

    ### get reftime (needed for relative hindcast year)
    inferred_reftime = _infer_reftime_from_filename(ds.encoding["source"])

    ### control or perturbed forecast?

    if "number" in ds.dims:
        pass
    else:
        ds = ds.assign_coords(number=[0])

    ### realtime forecast or hindcast?

    max_timesteps_of_one_forecast = 100
    dstime = ds.time

    is_realtime = len(dstime) < max_timesteps_of_one_forecast

    if is_realtime:
        # --> realtime
        reftime = inferred_reftime or ds.time[0].values
        # determine leadtime
        leadtime = (ds.time.values - reftime).astype("timedelta64")
        ds = ds.assign_coords(leadtime=("time", leadtime)).swap_dims(time="leadtime")
        ds = ds.rename(time="validtime")
        ds = ds.assign_coords(hc_year=[0])
    else:
        # --> hindcast
        delta_t = np.diff(ds.time).astype("timedelta64[D]")
        min_days_between_hc_years = 10
        idx_large_delta_t = (
                np.argwhere(
                    delta_t > np.timedelta64(min_days_between_hc_years, "D")
                ).ravel()
                + 1
        )
        reftime = inferred_reftime or ds.time.isel(time=idx_large_delta_t[-1]).values

        # determine leadtime
        time_of_first_fc = ds.time.isel(time=slice(0, idx_large_delta_t[0]))
        nt_onefc = len(time_of_first_fc)
        leadtime = (time_of_first_fc - ds.time[0]).values.astype("timedelta64[D]")

        # reshape time and determine hindcast years
        n_hcy = len(ds.time) // nt_onefc
        assert nt_onefc * n_hcy == len(ds.time), "doesnt match"
        reshaped_time = ds.time.values.reshape(
            n_hcy, nt_onefc
        )  # shape: hc_year, leadtime
        # actual hindcast year
        hcy = pd.DatetimeIndex(reshaped_time[:, 0]).year

        # hindcast year relative to reftime
        relative_hcy = hcy - pd.Timestamp(reftime).year

        leadtime_broadcasted = np.broadcast_to(leadtime, reshaped_time.shape).ravel()
        hcy_broadcasted = np.broadcast_to(relative_hcy, reshaped_time.T.shape).ravel(
            order="F"
        )

        ds = ds.assign_coords(
            leadtime=("time", leadtime_broadcasted),
            hc_year=("time", hcy_broadcasted),
        )

        ds = ds.set_index(time=["leadtime", "hc_year"])
        ds = ds.assign_coords(validtime=("time", dstime.values))
        ds = ds.unstack()
    ds = ds.assign_coords(reftime=[reftime])
    return ds


def _infer_reftime_from_filename(filepath):
    # split filepath to get filename
    filename = filepath.split("/")[-1]
    # split underscores, one item is probability the date
    filename_underscore_splitted = filename.split("_")
    # usually the second to last item is the date, therefore roll to save time
    filename_underscore_splitted = (
            filename_underscore_splitted[-2:] + filename_underscore_splitted[:-2]
    )
    # split points
    filename_underscore_splitted_point_splitted = [
        i.split('.') for i in filename_underscore_splitted
    ]
    filename_underscore_splitted_point_splitted = _flatten_list(filename_underscore_splitted_point_splitted)

    # try to parse reftime from one of these items
    for item in filename_underscore_splitted_point_splitted:
        try:
            inferred_reftime = pd.Timestamp(item)
            inferred_reftime = np.datetime64(inferred_reftime)
            break
        except ValueError:
            continue
        except:
            print("unknown error")
    else:
        inferred_reftime = None
        print(
            "unable to identify correct reftime!"
            + f"Infering reftime from one of these items: {filename_underscore_splitted_point_splitted} failed."
            + "Make sure that the reftime appears in the filename, otherwise can't infer correct reftime."
            + "I mean, we could specify the reftime, e.g., as a list, but I'm not sure if that's convenient... let me know if yes."
            + "\nValid file names are for example s2s_something_else_2017-11-01_foo_bar.nc, s2s_something_else_20171101.nc"
        )
    return inferred_reftime


def _flatten_list(lst):
    """
    Convert a list of lists to a flattened list.

    Parameters
    ----------
    lst

    Returns
    -------
    flat_list : list
    """
    for item in lst:
        if isinstance(item, list):
            yield from _flatten_list(item)
        else:
            yield item


def download_table_ecmwf_model():
    path = "https://confluence.ecmwf.int/display/S2S/ECMWF+Model"
    table_ecmwf_model_raw = pd.read_html(path)[0]

    table_ecmwf_model = table_ecmwf_model_raw.iloc[1:]
    table_ecmwf_model.loc[:, IMPL_DATE] = pd.to_datetime(
        table_ecmwf_model[IMPL_DATE], infer_datetime_format=True
    )
    return table_ecmwf_model


def model_version_of_init_date(date, table_ecmwf_model):
    diff = date - table_ecmwf_model[IMPL_DATE]
    return table_ecmwf_model[diff >= pd.Timedelta("0D")].iloc[0]["Model version"]


def add_model_cycle_ecmwf(ds):
    """
    Add a coordinate ``cycle`` to a dataset that denotes the ecmwf model cycle.

    Parameters
    ----------
    ds : xr.Dataset
        ecmwf s2s forecast data

    Returns
    -------
    ds : xr.Dataset
        dataset with new coordinate
    """

    try:
        import lxml
    except ImportError:
        print("lxml required, consider pip install lxml")
    else:
        table_ecmwf_model = download_table_ecmwf_model()
        mv = []
        for d in ds.reftime:
            mv.append(model_version_of_init_date(d.values, table_ecmwf_model))
        return ds.assign_coords(cycle=("reftime", mv))


def _reindex_reanalysis_with_s2s_valid_dates(s2s, reanalysis):
    """
    Reindex era5 to available s2s valid dates

    Parameters
    ----------
    s2s
    reanalysis

    Returns
    -------
    data : xr.DataArray | xr.Dataset
    """
    validtime_flattened_dates = np.unique(s2s.validtime.values.flatten())
    reanalysis_padded = reanalysis.reindex(time=validtime_flattened_dates)
    return reanalysis_padded


def concat_era5_before_s2s(s2s: xr.DataArray, era5: xr.DataArray, max_neg_leadtime_days: int = 46) -> xr.DataArray:
    """
    Append ERA5 prior to start of forecasts, ERA5 is indicated as negative leadtimes.

    Parameters
    ----------
    s2s : xr.DataArray
    era5 : xr.DataArray
        requires dimension ``time``
    max_neg_leadtime_days : int
        maximum negative leadtime (i.e. number of ERA5 days to append)

    Returns
    -------
    da : xr.DataArray
        dataset with s2s and era5 combined
    """
    assert s2s.name == era5.name

    # memory warning if dataset has dimension number
    if 'number' in s2s.dims:
        if len(s2s.number) > 1:
            warn(
                'If dimension number in S2S dataset, then padding ERA5 before forecast start leads to considerable' \
                'memory usage, as all ensemble members are padded with the same values.',
                ResourceWarning
            )

    # reindex s2s forecasts to negative lags
    s2s_with_neg_leadtimes = add_validtime(
        s2s.drop("validtime").reindex(
            leadtime=pd.timedelta_range(f"-{max_neg_leadtime_days}D", "-1D")
        )
    )

    era5_padded = _reindex_reanalysis_with_s2s_valid_dates(s2s_with_neg_leadtimes, era5)

    era5_on_s2s_structure = era5_padded.sel(time=s2s_with_neg_leadtimes.validtime)
    era5_on_s2s_structure_with_number = (
        era5_on_s2s_structure.drop("time")
        .expand_dims("number")
        .assign_coords(number=[0])
        .reindex(number=s2s.number, method="nearest")
    )
    s2s_and_era5 = xr.concat(
        [
            s2s,
            era5_on_s2s_structure_with_number,
        ],
        dim="leadtime",
    ).sortby("leadtime")
    return s2s_and_era5


def add_validtime(da):
    """
    Given a DataArray/ Dataset with dimensions ('reftime', 'hc_year', 'leadtime'), add a coordinate validtime that
    indicates the target date of the forecast. Example: reftime="2000-01-01", hc_year=-1, leadtime=+3D corresponds
    to validtime "1999-01-03".

    Parameters
    ----------
    da : xr.DataArray or xr.Dataset
        Input data, requires dimensions ('reftime', 'hc_year', 'leadtime').

    Returns
    -------
    xr.DataArray or xr.Dataset
        Same dataset as input, but with coordinate validtime.

    Notes
    -------
    Validtime is of type `np.datetime64` and it will not be a dimension.

    Warnings
    _______
    Only dimension `leadtime` is supported, not `days_since_init`.


    Warnings
    _______
    Only makes sense for ECMWF data.
    """
    da_stacked = da.stack(day=("reftime", "hc_year", "leadtime"))
    fc_day = (
            add_years(da_stacked.reftime.values, da_stacked.hc_year.values)
            + da_stacked.leadtime.values
    )

    fc_day_reshaped = fc_day.reshape(len(da.reftime), len(da.hc_year), len(da.leadtime))
    res = da.assign_coords(
        validtime=(("reftime", "hc_year", "leadtime"), fc_day_reshaped)
    )
    return res


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
        dir_incl_subdir = dir_excl_subdir + "/" + create_subdirectory  # e.g.: /foo/bar
        Path(dir_incl_subdir).mkdir(parents=True, exist_ok=False)
        path = dir_incl_subdir + "/" + path.split("/")[-1]  # e.g.: /foo/bar/filename

    reftimes, datasets = zip(*data.groupby("reftime", squeeze=False))
    # print(datasets[0])
    # datasets = [d.expand_dims("reftime") for d in datasets]
    paths = [
        f"{path}_{f}.nc"
        for f in pd.DatetimeIndex(reftimes).strftime("%Y-%m-%d")
    ]
    result = xr.save_mfdataset(datasets, paths, compute=False)

    try:
        from dask.diagnostics import ProgressBar
    except ImportError:
        result.compute()
    else:
        with ProgressBar():
            result.compute()


def stack_fc(d, reset_index=True):
    """
    Go from dimensions (``reftime``, ``hc_year``, ``number``) to dimension ``fc``.

    Parameters
    ----------
    d : xr.DataArray | xr.Dataset
    reset_index : bool
        If True, drop multiindex and flatten around new index ``fc``.

    Returns
    -------
    data : xr.DataArray | xr.Dataset
    """
    if reset_index:
        return d.stack(fc=("reftime", "hc_year", "number")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year", "number"))


def stack_ensfc(d, reset_index=True):
    """
    Go from dimensions (``reftime``, ``hc_year``) to dimension ``fc``.

    Parameters
    ----------
    d : xr.DataArray | xr.Dataset
    reset_index : bool
        If True, drop multiindex and flatten around new index ``fc``.

    Returns
    -------
    data : xr.DataArray | xr.Dataset
    """
    if reset_index:
        return d.stack(fc=("reftime", "hc_year")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year"))


def combine_s2s_and_reanalysis(s2s, reanalysis, ensfc=True):
    """
    Project reanalysis time series on S2S forecast data. Resulting object will have dimensions of s2s dataset.

    Parameters
    ----------
    s2s : xr.Dataset | xr.DataArray
    reanalysis : xr.Dataset | xr.DataArray
    ensfc : bool
        If True, stack resulting forecasts to ensemble forecasts ([reftime, hc_year] -> fc)

    Returns
    -------
    combined_data : xr.Dataset

    Examples
    --------
    >>> ds_s2s
    <xarray.Dataset>
    Dimensions:    (leadtime: 47, longitude: 2, latitude: 1, number: 51,
                    reftime: 2, hc_year: 21)
    Coordinates:
      * leadtime   (leadtime) timedelta64[ns] 0 days 1 days ... 45 days 46 days
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
      * number     (number) int64 0 1 2 3 4 5 6 7 8 9 ... 42 43 44 45 46 47 48 49 50
      * reftime    (reftime) datetime64[ns] 2017-11-16 2017-11-20
      * hc_year    (hc_year) int64 -20 -19 -18 -17 -16 -15 -14 ... -5 -4 -3 -2 -1 0
        validtime  (reftime, leadtime, hc_year) datetime64[ns] 1997-11-16 ... 201...
    Data variables:
        u          (reftime, latitude, longitude, leadtime, hc_year, number) float32 dask.array<chunksize=(1, 1, 2, 47, 20, 1), meta=np.ndarray>
    >>> ds_reanalysis
    <xarray.Dataset>
    Dimensions:    (time: 30, latitude: 1, longitude: 2)
    Coordinates:
      * time       (time) datetime64[ns] 2017-11-01 2017-11-02 ... 2017-11-30
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
    Data variables:
        u          (time, latitude, longitude) float32 dask.array<chunksize=(30, 1, 2), meta=np.ndarray>
    >>> import s2stools.process
    >>> s2stools.process.combine_s2s_and_reanalysis(s2s, reanalysis)
    <xarray.Dataset>
    Dimensions:    (leadtime: 47, longitude: 2, latitude: 1, number: 51,
                    reftime: 2, hc_year: 21)
    Coordinates:
      * leadtime   (leadtime) timedelta64[ns] 0 days 1 days ... 45 days 46 days
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
      * number     (number) int64 0 1 2 3 4 5 6 7 8 9 ... 42 43 44 45 46 47 48 49 50
      * reftime    (reftime) datetime64[ns] 2017-11-16 2017-11-20
      * hc_year    (hc_year) int64 -20 -19 -18 -17 -16 -15 -14 ... -5 -4 -3 -2 -1 0
        validtime  (reftime, leadtime, hc_year) datetime64[ns] 1997-11-16 ... 201...
    Data variables:
        u          (reftime, latitude, longitude, leadtime, hc_year, number) float32 dask.array<chunksize=(1, 1, 2, 47, 20, 1), meta=np.ndarray>
        u_verif    (reftime, leadtime, hc_year, latitude, longitude) float32 dask.array<chunksize=(2, 47, 21, 1, 2), meta=np.ndarray>

    See Also
    --------
    :func:`concat_era5_before_s2s`
    """
    if 'validtime' not in s2s.coords:
        s2s = add_validtime(s2s)

    reanalysis_padded = _reindex_reanalysis_with_s2s_valid_dates(s2s, reanalysis)

    # project reanalysis onto s22 structure
    reanalysis_s2s_structure = reanalysis_padded.sel(time=s2s.validtime).drop("time")

    # merge (and give reanalysis variables new names by adding "_verif")
    try:
        ifs_with_verif = xr.merge([s2s, reanalysis_s2s_structure])
    except xr.core.merge.MergeError:
        print('Renaming reanalysis variables by adding _verif (for verification)')
        if isinstance(reanalysis_s2s_structure, xr.DataArray):
            new_name = reanalysis_s2s_structure.name + "_verif"
            reanalysis_s2s_structure = reanalysis_s2s_structure.rename(new_name)
        elif isinstance(reanalysis_s2s_structure, xr.Dataset):
            for n in reanalysis_s2s_structure.data_vars:
                new_name = str(n) + "_verif"
                reanalysis_s2s_structure = reanalysis_s2s_structure.rename({n: new_name})

        ifs_with_verif = xr.merge([s2s, reanalysis_s2s_structure])

    if ensfc:
        # stack new dataset to ensfc
        ifs_with_verif_ensfc = ifs_with_verif.stack(fc=["reftime", "hc_year"])
        return ifs_with_verif_ensfc
    else:
        return ifs_with_verif
