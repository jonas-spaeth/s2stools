import dask.array
import numpy as np
import scipy.stats
import xarray as xr
import scipy.signal


def mode(obj, dim):
    """
    Compute mode

    Parameters
    ----------
    obj : (xr.Dataset | xr.DataArray)
        input data
    dim : str
        dimension for computing the mode

    Returns
    -------
    mode : xr.Dataset
        Mode of the input dataset along dimension `dim`

    See Also
    --------
    :func:`scipy.stats.mode`

    """
    # note: apply always moves core dimensions to the end
    # usually axis is simply -1 but scipy's mode function doesn't seem to like that
    # this means that this version will only work for DataArray's (not Datasets)
    assert isinstance(obj, xr.DataArray)
    axis = obj.ndim - 1
    return xr.apply_ufunc(_mode, obj, input_core_dims=[[dim]], kwargs={"axis": axis})


def _mode(*args, **kwargs):
    vals = scipy.stats.mode(*args, **kwargs, nan_policy="omit")
    # only return the mode (discard the count)
    return vals[0].squeeze()


def zonal_wavenumber_decomposition(data, k_aggregates=True):
    """
    Decompose data into zonal wavenumber components (k0=mean, k1=amplitude of lowest frequency). Applies a fft along 'longitude' and introduces dimension k.

    Parameters
    ----------
        data: xr.DataArray or xr.Dataset
            Data for wavenumber decomposition.
        k_aggregates: boolean or dict
            If True, dimension k has coordinates '0', '1', '2', '3', '4-7', '8-20', '21-inf' where k-ranges
            contain the sum over these wavenumbers. If False, return full wavenumber components and k will
            have integer values as coordinate.
            If dict, apply a custom k_aggregate of the form {0: '0', slice(1-3): '1to3', ...}. Defaults to True.

    Returns
    -------
        xr.DataArray or xr.Dataset

    Warnings
    --------
        xr.DataArray not yet supported as input.
    """

    assert 'longitude' in data.dims, f'longitude not in dimensions {data.dims}'
    assert isinstance(data.data, dask.array.Array), f'data needs to be chunked (but data is of type {type(data)})'

    n = len(data.longitude)
    dx = data.longitude[1].values - data.longitude[0].values
    freqs = np.fft.rfftfreq(n, dx)

    data_fft = xr.apply_ufunc(
        np.fft.rfft,
        data,
        kwargs=dict(norm="forward"),
        input_core_dims=[["longitude"]],
        output_core_dims=[["k"]],
        dask="parallelized",
        dask_gufunc_kwargs=dict(output_sizes=dict(k=len(freqs))),
    )
    data_fft = data_fft.assign_coords(k=data_fft.k.values)

    # due to symmetry of frequencies, multiply all ks by a factor of 2, except for k=0
    # data_fft = data_fft.where(data_fft.k == 0, other=2 * data_fft)

    if isinstance(k_aggregates, bool):
        if k_aggregates:
            k_aggregates = None  # None will fall back to standard aggragation later
        else:
            return data_fft
    else:
        assert isinstance(k_aggregates,
                          dict), f"unsupported type for k_aggregates (needs to be one of (bool, dict), not {type(k_aggregates)})"

    # k aggregation
    return aggregate_k(data_fft, k_aggregates)


def eddy_flux_spectral(a, b, aggregate_k=None, verify_that_sum_over_k_is_total_flux=False,
                       return_two_profiles_along_dim=False):
    """
    Compute spectral covariance, e.g. for eddy heatflux.

    Parameters
    ----------
    a : xr.DataArray
    b : xr.DataArray
    aggregate_k : dict
        rule to combine wavenumbers, e.g., {'4-7': slice(4, 7). Defaults to 0, 1, 2, 3, 4-7, 8-20, 21+.
        If False, don't combine.}
    verify_that_sum_over_k_is_total_flux
    return_two_profiles_along_dim

    Returns
    -------

    """
    assert 'longitude' in a.dims, "a requires dimension longitude"
    assert 'longitude' in b.dims, "b requires dimension longitude"

    # zonal anomalies (p is for 'prime')
    ap = a - a.mean('longitude')
    bp = b - b.mean('longitude')

    # wavenumber decomposition
    a_fft = zonal_wavenumber_decomposition(ap)
    b_fft = zonal_wavenumber_decomposition(bp)

    # flux
    ab_fft = np.real(a_fft * b_fft.conj() + a_fft.conj() * b_fft)

    # aggregate k
    ab_fft = aggregate_k(ab_fft, rule=aggregate_k) if aggregate_k is not None else ab_fft

    # verify that sum over k is equal to total flux
    if verify_that_sum_over_k_is_total_flux:
        ab_total_flux = (ap * bp).mean('longitude')
        other_dims_first = {d: 0 for d in ab_total_flux.dims if (d != 'longitude') & (d != 'leadtime')}
        ab_total_flux_one_longitude_profile = ab_total_flux.isel(other_dims_first)
        ab_fft_one_longitude_profile = ab_fft.isel(other_dims_first).sum('k')
        mean_diff = (ab_total_flux_one_longitude_profile - ab_fft_one_longitude_profile).mean()
        assert mean_diff < 10, f"Sum over k is not equal to total flux (maximum difference is {mean_diff.values})"

        if return_two_profiles_along_dim:
            return ab_fft, (ab_total_flux_one_longitude_profile, ab_fft_one_longitude_profile)
    else:
        if return_two_profiles_along_dim:
            print("Cannot return return_two_profiles_along_dim when verify_that_sum_over_k_is_total_flux is not True.")

    return ab_fft


def aggregate_k(data, rule=None):
    """
    Aggregate certain k's to a wavenumber range, corresponding to the sum.
    Parameters
    ----------
    data : xr.Dataset or xr.DataArray
    rule : dict
        If None, use default aggregation: 0, 1, 2, 3, 4-7, 8-20, 21-inf.
    Returns
    -------
    xr.Dataset or xr.DataArray
    """

    if rule is None:
        rule = {
            "0": 0,
            "1": 1,
            "2": 2,
            "3": 3,
            "4-7": slice(4, 7),
            "8-20": slice(8, 20),
            "21-inf": slice(21, None),
        }

    assert 'k' in data.dims, f"'k' is not one of the dimensions in data: {data.dims}"
    to_merge = []
    for new_k_name, k_range in rule.items():
        data_sel = data.sel(k=k_range)
        #        print(data_sel)
        if "k" in data_sel.dims:
            # print("-->")
            # min_count=1 is important, because sum over NaN otherwise yields 0, which must be avoided, especially when computing anomalies
            data_sel = data_sel.sum("k", min_count=1)
        to_merge.append(data_sel.expand_dims("k").assign_coords(k=[new_k_name]))
    data_aggr_k = xr.concat(to_merge, dim="k")

    return data_aggr_k


def _conv_mat(windows, n):
    rows = []
    for w in np.atleast_1d(windows):
        row = _conv_arr(w, n)
        rows.append(row)
    return np.stack(rows)


def _conv_arr(window, n):
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


def _valid_mask_mat(windows, n):
    rows = []
    for w in np.atleast_1d(windows):
        row = _valid_mask_arr(w, n)
        rows.append(row)
    return np.stack(rows)


def _valid_mask_arr(window, n):
    vma = _conv_arr(n - window + 1, n) > 0
    flip = n % 2 == 0
    if flip:
        vma = np.flip(vma, axis=0)
    return vma


def _running_mean(array, windows):
    # derived parameters
    n = array.shape[-1]
    n_windows = len(windows)

    # calculation
    mat = _conv_mat(windows, n)  # shape: (n_windows, n)
    array_bc = np.broadcast_to(
        array, shape=((n_windows,) + array.shape)
    )  # shape: (n_windows, ..., n)

    mat_bc = np.broadcast_to(
        mat.T, shape=(array.shape + (n_windows,))
    )  # (..., n, n_windows)
    mat = np.moveaxis(mat_bc, -1, 0)  # shape: (n_windows, ..., n)

    res = scipy.signal.fftconvolve(array_bc, mat, mode="same", axes=-1).round(2)

    valid_mask = _valid_mask_mat(windows, n)
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
        window_sizes (list): List of window sizes to use. If  only a single value, it probably makes sense to see xarrays ``xr.DataArray.running().mean()`` instead.

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
