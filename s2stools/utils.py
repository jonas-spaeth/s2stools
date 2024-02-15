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


def wrap_time(xr_data, season_start_month=12, change_dims=True):
    # year of season (index corresponds to lower bound of season)
    coord_season_yr = xr.where(
        xr_data.time.dt.month < season_start_month,
        xr_data.time.dt.year - 1,
        xr_data.time.dt.year,
    )

    # day of season (first day of season is 1)
    season_start_doy_in_non_leap = pd.Timestamp(
        f"1999-{season_start_month}-01"
    ).dayofyear

    # add 1 to season start day if year is leap
    season_start_yr_is_leap = (coord_season_yr % 4) == 0
    if season_start_month > 2:
        season_start_doy = season_start_doy_in_non_leap + season_start_yr_is_leap
    else:
        season_start_doy = season_start_doy_in_non_leap

    # (add 1 in order to let Nov 1 be day of season = 1 and not =0)
    coord_season_dof = (
        (
            xr_data.time.dt.dayofyear
            + xr.where(
                xr_data.time.dt.dayofyear < season_start_doy,
                365 + season_start_yr_is_leap,
                0,
            )
        )
        % season_start_doy
    ) + 1

    # store old time coordinates
    time_idx = xr_data.time.values

    xr_data_newcoords = xr_data.assign_coords(
        season=("time", coord_season_yr.values),
        dayofseason=("time", coord_season_dof.values),
        season_start_month=season_start_month,
    )

    if not change_dims:
        return xr_data_newcoords
    else:
        xr_data_newidx = xr_data_newcoords.set_index(
            time=("season", "dayofseason")
        ).rename(time="stacked_time")

        # add old time index again
        xr_data_final = xr_data_newidx.assign_coords(
            time=("stacked_time", time_idx)
        ).unstack()

        return xr_data_final


def unwrap_time(xr_data, season_start_month=None):
    if season_start_month is None:
        season_start_month = int(xr_data.season_start_month)
        assert season_start_month > 0 and season_start_month < 13

    seasons = xr_data.season
    dates_first_of_jan = pd.to_datetime(seasons.values, format="%Y")
    dates_season_starts = dates_first_of_jan + pd.tseries.offsets.DateOffset(
        months=season_start_month - 1
    )

    dates_season_starts_xr = xr.DataArray(dates_season_starts, dims="season")
    deltas = pd.TimedeltaIndex(xr_data.dayofseason.values - 1, unit="D")
    deltas_xr = xr.DataArray(deltas, dims="dayofseason")

    xr_data_stacked = xr_data.stack(time=("season", "dayofseason"))
    time_stacked = (dates_season_starts_xr + deltas_xr).stack(
        time=("season", "dayofseason")
    )
    xr_data_stacked_time = xr_data_stacked.assign_coords(time=time_stacked.values)
    return xr_data_stacked_time

    # dos = xr_data.dayofseason


def wrap_time2(xr_data, season_start_month, change_dims=True):
    grouper = xr.where(
        xr_data.time.dt.month < season_start_month,
        xr_data.time.dt.year - 1,
        xr_data.time.dt.year,
    )
    grouped = xr_data.groupby(grouper)

    def time_to_dayofseason(x):
        dos = np.arange(1, len(x.time) + 1)
        x_new_coord = x.assign_coords(timestepofseason=("time", dos))
        x_new_idx = x_new_coord.swap_dims({"time": "timestepofseason"})
        return x_new_idx

    grouped_newidx = grouped.map(time_to_dayofseason)

    return grouped_newidx.assign_coords(season_start_month=season_start_month)


def unwrap_time2(xr_data):
    grouped = xr_data.groupby("time")
    return grouped.first(skipna=True)
