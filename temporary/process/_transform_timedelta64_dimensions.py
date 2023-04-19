import xarray as xr
import numpy as np


def leadtime_to_dayssinceinit(x):
    kwargs = dict(dim_timedelta="leadtime", dim_integer_days="days_since_init")
    if isinstance(x, xr.DataArray):
        return _dataarray_from_timedelta_to_integer_days(x, **kwargs)
    elif isinstance(x, xr.Dataset):
        return x.map(
            _dataarray_from_timedelta_to_integer_days,
            **kwargs,
        )
    else:
        raise (f"invalid datatype of x, must be DataArray or Dataset, not {type(x)}")


def dayssinceinit_to_leadtime(x):
    """
    transform the dimension 'days_since_init' (dtype=int) of a dataset or
    dataarray to the dimension 'leadtime' (dtype=np.timedelta64)

    see also: leadtime_to_dayssinceinit, lagtime_to_dayssinceevent
    """
    kwargs = dict(dim_integer_days="days_since_init", dim_timedelta="leadtime")
    if isinstance(x, xr.DataArray):
        return _dataarray_from_integer_days_to_timedelta(x, **kwargs)
    elif isinstance(x, xr.Dataset):
        return x.map(_dataarray_from_integer_days_to_timedelta, **kwargs)
    else:
        raise (f"invalid datatype of x, must be DataArray or Dataset, not {type(x)}")


def lagtime_to_dayssinceevent(x):
    kwargs = dict(dim_timedelta="lagtime", dim_integer_days="days_since_event")
    if isinstance(x, xr.DataArray):
        return _dataarray_from_timedelta_to_integer_days(x, **kwargs)
    elif isinstance(x, xr.Dataset):
        return x.map(
            _dataarray_from_timedelta_to_integer_days,
            **kwargs,
        )
    else:
        raise (f"invalid datatype of x, must be DataArray or Dataset, not {type(x)}")


def dayssinceevent_to_lagtime(x):
    kwargs = dict(dim_integer_days="days_since_event", dim_timedelta="lagtime")
    if isinstance(x, xr.DataArray):
        return _dataarray_from_integer_days_to_timedelta(x, **kwargs)
    elif isinstance(x, xr.Dataset):
        return x.map(_dataarray_from_integer_days_to_timedelta, **kwargs)
    else:
        raise (f"invalid datatype of x, must be DataArray or Dataset, not {type(x)}")


def _dataarray_from_integer_days_to_timedelta(data, dim_integer_days, dim_timedelta):
    # is day-dimension in dimensions of data?
    if dim_integer_days in data.dims:
        # compute new timedeltas based on integers-days
        timedeltas = data[dim_integer_days].values.astype("timedelta64[D]")
        # change coordinates to timedelta
        data = data.assign_coords({dim_integer_days: timedeltas})
        # rename from integer to timedelta dimension
        data = data.rename({dim_integer_days: dim_timedelta})
    # drop days dimension if necessary
    if dim_integer_days in data.dims:
        data = data.drop(dim_integer_days)
    return data


def _dataarray_from_timedelta_to_integer_days(data, dim_timedelta, dim_integer_days):
    # is day-dimension in dimensions of data?
    if dim_timedelta in data.dims:
        # compute new integers-days based on timedeltas
        integer_days = data[dim_timedelta].values.astype("timedelta64[D]").astype("int")
        # change coordinates to timedelta
        data = data.assign_coords({dim_timedelta: integer_days})
        # rename from integer to timedelta dimension
        data = data.rename({dim_timedelta: dim_integer_days})
    # drop days dimension if necessary
    if dim_timedelta in data.dims:
        data = data.drop(dim_timedelta)
    return data