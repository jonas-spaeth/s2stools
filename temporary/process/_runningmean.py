import scipy.signal
import numpy as np
import xarray as xr


def conv_mat(windows, n):
    rows = []
    for w in np.atleast_1d(windows):
        row = conv_arr(w, n)
        rows.append(row)
    return np.stack(rows)


def conv_arr(window, n):
    assert (
        window <= n
    ), f"window ({window}) must smaller or equal to the dimension length n ({n})"
    n_zeros_left = np.ceil((n - window) / 2).astype("int")
    n_zeros_right = np.floor((n - window) / 2).astype("int")

    if n % 2 == 0:
        n_zeros_left, n_zeros_right = n_zeros_right, n_zeros_left

    n_ones = window
    return np.concatenate(
        [np.zeros(n_zeros_left), np.ones(n_ones) / n_ones, np.zeros(n_zeros_right)]
    )


def valid_mask_mat(windows, n):
    rows = []
    for w in np.atleast_1d(windows):
        row = valid_mask_arr(w, n)
        rows.append(row)
    return np.stack(rows)


def valid_mask_arr(window, n):
    vma = conv_arr(n - window + 1, n) > 0
    flip = n % 2 == 0
    if flip:
        vma = np.flip(vma, axis=0)
    return vma


def _running_mean(array, windows):
    # derived parameters
    n = array.shape[-1]
    n_windows = len(windows)

    # calculation
    mat = conv_mat(windows, n)  # shape: (n_windows, n)
    array_bc = np.broadcast_to(
        array, shape=((n_windows,) + array.shape)
    )  # shape: (n_windows, ..., n)

    mat_bc = np.broadcast_to(
        mat.T, shape=(array.shape + (n_windows,))
    )  # (..., n, n_windows)
    mat = np.moveaxis(mat_bc, -1, 0)  # shape: (n_windows, ..., n)

    res = scipy.signal.fftconvolve(array_bc, mat, mode="same", axes=-1).round(2)

    valid_mask = valid_mask_mat(windows, n)
    valid_mask_bc = np.broadcast_to(
        valid_mask.T, shape=(array.shape + (n_windows,))
    )  # (..., n, n_windows)
    valid_mask = np.moveaxis(valid_mask_bc, -1, 0)  # shape: (n_windows, ..., n)

    res_masked = np.where(valid_mask, res, np.nan)
    return np.moveaxis(res_masked, 0, -1)


def running_mean(dataarray, dim, window_sizes):
    """Compute running mean (= moving average) of DataArray along a dimension. Use different window sizes and stack the result along a new dimension `window_size`.

    Args:
        dataarray (xr.DataArray): Input data.
        dim ('str'): Dimension across which running mean is computed.
        window_sizes (list): List of window sizes to use. If  only a single value, it probably makes sense to se xarrays `xr.DataArray.running().mean()` instead.

    Returns:
        xr.DataArray: Output data, which has new dimension `window_size` of length `len(window_sizes)`.
    """

    window_sizes = np.atleast_1d(window_sizes)
    return xr.apply_ufunc(
        _running_mean,
        dataarray,
        window_sizes,
        input_core_dims=[[dim], []],
        output_core_dims=[[dim, "window_size"]],
        dask_gufunc_kwargs=dict(
            output_sizes={"window_size": len(window_sizes), dim: len(dataarray[dim])},
            allow_rechunk=True,
        ),
        dask="allowed",
    ).assign_coords(window_size=window_sizes)
