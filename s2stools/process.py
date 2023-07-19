import numpy as np
import pandas as pd
import xarray as xr


def s2sparser(ds):
    """
    Will create dimensions reftime, hc_year, leadtime.
    Coordinate validtime is automatically added.

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
    # try to parse reftime from one of these items
    for item in filename_underscore_splitted:
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
            + f"Infering reftime from one of these items: {filename_underscore_splitted} failed."
            + "Make sure that the reftime appears in the filename, otherwise can't infer correct reftime."
            + "I mean, we could specify the reftime, e.g., as a list, but I'm not sure if that's convenient... let me know if yes."
        )
    return inferred_reftime
