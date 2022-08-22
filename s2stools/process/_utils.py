import scipy.stats
import xarray as xr
import numpy as np


def stack_fc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year", "number")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year", "number"))


def stack_ensfc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year"))


def mode(obj, dim):
    # note: apply always moves core dimensions to the end
    # usually axis is simply -1 but scipy's mode function doesn't seem to like that
    # this means that this version will only work for DataArray's (not Datasets)
    assert isinstance(obj, xr.DataArray)
    axis = obj.ndim - 1
    return xr.apply_ufunc(_mode, obj,input_core_dims=[[dim]],kwargs={'axis': axis})


def _mode(*args, **kwargs):
    vals = scipy.stats.mode(*args, **kwargs, nan_policy='omit')
    # only return the mode (discard the count)
    return vals[0].squeeze()


def split_reftimes_with_gap(data, minimum_gap_to_split_in_days=10, hide_print=True):
    """
    separate a single dataset (or dataarray) into a list of datasets when two consecutive reftimes are
    further apart than a certain number of days. for example suitable to separate different winters.
    """
    diff = np.diff(data.reftime).astype("timedelta64[D]")
    idx_gaps = np.argwhere(diff > np.timedelta64(10, "D")).ravel()

    gap_start = 0
    result = []
    for gap_end in idx_gaps + 1:
        result.append(data.isel(reftime=slice(gap_start, gap_end)))
        gap_start = gap_end

    result.append(data.isel(reftime=slice(gap_start, None)))
    if not hide_print:
        _ = [print(f"{group.reftime[0]} to {group.reftime[-1]}") for group in result]
    return result