import calendar
import numpy as np
import pandas as pd
import calendar
import xarray as xr


def list_to_string(l):
    if isinstance(l, str):
        return l
    if isinstance(l, range) | isinstance(l, np.ndarray):
        l = list(l)
    if isinstance(l, list):
        return "/".join([str(i) for i in l])
    else:
        assert False, "list_to_string excepts only type string or list, not {}".format(
            type(l)
        )


def add_years(dt64, years):
    """
    Add year to date.

    Args:
        dt64 ([datetime64], datetime64): date to change
        years ([int], int): how many years to add. If scalar, add same year to each date, if list then years must have same shape as dt64.

    Returns:
        new date(s) with same shape as dt64
    """
    pd_dt = pd.to_datetime(dt64)
    if len(np.atleast_1d(years)) == 1:

        def replace_year(x):
            if (
                (x.month == 2)
                & (x.day == 29)
                & (calendar.isleap(x.year + years) == False)
            ):
                return x.replace(year=x.year + years, day=28)
            else:
                return x.replace(year=x.year + years)

        res = pd.Series(pd_dt).apply(replace_year)
    else:
        assert np.array(dt64).shape == np.array(years).shape
        res = []
        for i in range(len(dt64)):
            pd_dt = pd.to_datetime(dt64[i])
            if (
                (pd_dt.month == 2)
                & (pd_dt.day == 29)
                & (calendar.isleap(pd_dt.year + years[i]) == False)
            ):
                res.append(pd_dt.replace(year=pd_dt.year + years[i], day=28))
            else:
                res.append(pd_dt.replace(year=pd_dt.year + years[i]))

    res = np.array(res, "datetime64[D]")
    if len(res) == 1:
        res = res[0]
    return res


def to_timedelta64(a, assume="D"):
    """
    Convert to numpy timedelta64

    Args:
        a (int or np.timedelta64): timedelta
        assume (): timedelta64 format that is assumed if a is of type int.

    Returns:
        np.timedelta64
    """
    if isinstance(a, int):
        a = np.timedelta64(a, "D")
    return a


def month_int_to_abbr(months):
    """
    convert e.g. [12, 1, 2] to ["Dec", "Jan", "Feb"]
    Args:
        months: one month or list of months as integers

    Returns:
        list of str
    """
    months = np.atleast_1d(months)
    d = {index: month for index, month in enumerate(calendar.month_abbr) if month}
    return [d[m] for m in months]


def lat_weighted_spatial_mean(dataarray, lon_name="longitude", lat_name="latitude"):
    """
    Spatial average over longitude, latitude. Gridpoints are weighted by cos(lat) to account for different areas on
    Gaussian grid on a sphere.

    Parameters
    ----------
    dataarray (xr.DataArray)
    lon_name (str) : name of longitude dimension
    lat_name (str) : name of latitude dimension

    Returns
    -------
    weighted_dataarray (xr.DataArray)
    """
    weights = np.cos(np.deg2rad(dataarray.latitude))
    spatial_dims = [d for d in (lon_name, lat_name) if d in dataarray.dims]
    return dataarray.weighted(weights).mean(spatial_dims)


def groupby_quantiles(dataarray, groupby, q, q_dim, labels=None):
    """
    xr.core.dataset.Dataset.groupby_bins(..., bins=N) groups the data into N groups which span same interval sizes.
    This method groups the data into `len(q)` bins, which have equal number of subdatasets in the respective groups.

    Parameters
    ----------
    dataarray (xr.Dataset | xr.DataArray) : the data to group
    groupby (xr.Dataset | xr.DataArray | str) : what to group by
    q (list) : list of quantiles
    q_dim (str) : dimension across which quantiles are computed
    labels ([str]) : labels to put on new groups; asserting len(q) == len(labels)

    Returns
    -------
    groupby_object : xarray.core.groupby.DatasetGroupBy | xarray.core.groupby.DataArrayGroupBy
    """
    q = groupby.quantile(q, dim=q_dim)
    q_bins = [-np.inf, *q.values.tolist(), np.inf]
    return dataarray.groupby_bins(groupby, bins=q_bins, labels=labels)


def wrap_time(xr_data, season_start_month, change_dims=True):
    """
    Transform data from a linear time axis to a reshaped pair of ``("season", "timestepofseason")``.
    Can for example be used to go from time to DJF and day since Dec 1.

    Parameters
    ----------
    xr_data (xr.Dataset | xr.DataArray) : data to transform
    season_start_month (int) : month when the season starts, between 1 and 12
    change_dims (bool) : if True, the time dimension is replaced by ``timestepofseason`` and ``season``.
     If False, the time dimension, and ``season`` and ``timestepofseason`` are just added as new coordinates.

    Returns
    -------
    xr_data : xr.Dataset | xr.DataArray

    Examples
    --------
    >>> times = pd.date_range("2000-11-01", "2001-12-01", freq="D")
    >>> nt = len(times)
    >>> ds = xr.DataArray(
    >>>     np.random.randint(low=0, high=100, size=(nt, 10)),
    >>>     coords=dict(time=times, x=np.arange(10)),
    >>>     dims=["time", "x"],
    >>> )
    >>> print(ds)
    <xarray.DataArray (time: 396, x: 10)>
    array([[27, 34, 84, ..., 68, 16, 61],
           [12, 98, 94, ..., 25, 38, 87],
           [ 4, 27, 91, ..., 78, 48, 55],
           ...,
           [ 1, 75, 64, ..., 21, 21,  7],
           [18, 96,  9, ..., 60, 85,  2],
           [31, 28, 36, ..., 64, 48, 97]])
    Coordinates:
      * time     (time) datetime64[ns] 2000-11-01 2000-11-02 ... 2001-12-01
      * x        (x) int64 0 1 2 3 4 5 6 7 8 9
    >>> ds_transformed = wrap_time(ds, season_start_month=11, change_dims=True)
    >>> print(ds_transformed)
    <xarray.DataArray (season: 2, x: 10, timestepofseason: 365)>
    array([[[46.,  3., 62., ..., 63., 15., 69.],
            [22., 23., 89., ..., 40., 47., 63.],
            ....
            [91.,  7., 78., ..., nan, nan, nan],
            [87., 30., 88., ..., nan, nan, nan]]])
    Coordinates:
      * x                   (x) int64 0 1 2 3 4 5 6 7 8 9
      * timestepofseason    (timestepofseason) int64 1 2 3 4 5 ... 362 363 364 365
        time                (season, timestepofseason) datetime64[ns] 2000-11-01 ...
      * season              (season) int64 2000 2001
        season_start_month  int64 11
    """
    # Grouper: season is the year of the first month of the season
    grouper = xr.where(
        xr_data.time.dt.month < season_start_month,
        xr_data.time.dt.year - 1,
        xr_data.time.dt.year,
    ).rename("season")

    # Group by season
    grouped = xr_data.groupby(grouper)

    # add coordinate timestepofseason
    def time_to_dayofseason(x):
        dos = np.arange(1, len(x.time) + 1)
        x_new_coord = x.assign_coords(timestepofseason=("time", dos))
        if change_dims:
            x_new_coord = x_new_coord.swap_dims({"time": "timestepofseason"})
        return x_new_coord

    grouped_newidx = grouped.map(time_to_dayofseason)

    # add season_start_month as coordinate, for info
    result = grouped_newidx.assign_coords(season_start_month=season_start_month)

    # if change_dims is False, add season manually as coordinate
    if not change_dims:
        result = result.assign_coords(season=("time", grouper.values))

    return result


def unwrap_time(xr_data):
    """
    Transform data back from ("season", "timestepofseason") to a linear time axis.

    Parameters
    ----------
    xr_data (xr.Dataset | xr.DataArray) : data to transform

    Returns
    -------
    xr_data : xr.Dataset | xr.DataArray

    See Also
    --------
    :func:`wrap_time`
    """
    grouped = xr_data.groupby("time")
    return grouped.first(skipna=True)
