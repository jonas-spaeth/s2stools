import calendar
import numpy as np
import pandas as pd
import calendar


def list_to_string(l):
    if isinstance(l, str):
        return l
    if isinstance(l, range) | isinstance(l, np.ndarray):
        l = list(l)
    if isinstance(l, list):
        return "/".join([str(i) for i in l])
    else:
        assert False, "list_to_string excepts only type string or list, not {}".format(type(l))


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
    Assume
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


def lat_weighted_spatial_mean(dataarray, lon_name='longitude', lat_name='latitude'):
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