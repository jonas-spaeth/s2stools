import scipy.stats
import xarray as xr


def _mode(*args, **kwargs):
    vals = scipy.stats.mode(*args, **kwargs, nan_policy="omit")
    # only return the mode (discard the count)
    return vals[0].squeeze()


def mode(obj, dim):
    # note: apply always moves core dimensions to the end
    # usually axis is simply -1 but scipy's mode function doesn't seem to like that
    # this means that this version will only work for DataArray's (not Datasets)
    assert isinstance(obj, xr.DataArray)
    axis = obj.ndim - 1
    return xr.apply_ufunc(_mode, obj, input_core_dims=[[dim]], kwargs={"axis": axis})
