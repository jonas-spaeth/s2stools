import xarray as xr
import numpy as np
import dask.array


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


def eddy_flux_spectral(a, b, aggregate_k=None, verify_that_sum_over_k_is_total_flux=False, return_two_profiles_along_dim=False):
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
