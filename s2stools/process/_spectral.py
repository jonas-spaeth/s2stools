import xarray as xr
import numpy as np
import dask.array


def zonal_wavenumber_decomposition(data, k_aggregates=True):
    assert 'longitude' in data.dims, f'longitude not in dimensions {data.dims}'
    assert isinstance(data.data, dask.array.Array), f'data needs to be chunked (but data is of type {type(data)})'

    n = len(data.longitude)
    dx = data.longitude[1].values - data.longitude[0].values
    freqs = np.fft.rfftfreq(n, dx)

    data_fft = np.abs(
        xr.apply_ufunc(
            np.fft.rfft,
            data,
            kwargs=dict(norm="forward"),
            input_core_dims=[["longitude"]],
            output_core_dims=[["k"]],
            dask="parallelized",
            dask_gufunc_kwargs=dict(output_sizes=dict(k=len(freqs))),
        )
    )
    data_fft = data_fft.assign_coords(k=data_fft.k.values)

    # due to symmetry of frequencies, multiply all ks by a factor of 2, except for k=0
    data_fft = data_fft.where(data_fft.k == 0, other=2 * data_fft)

    if isinstance(k_aggregates, bool):
        if k_aggregates:
            k_aggregates = {
                "0": 0,
                "1": 1,
                "2": 2,
                "3": 3,
                "4-7": slice(4, 7),
                "8-20": slice(8, 20),
                "21-inf": slice(21, None),
            }
        else:
            return data_fft
    else:
        assert isinstance(k_aggregates, dict), f"unsupported type for k_aggregates (needs to be one of (bool, dict), not {type(k_aggregates)})"

    # k aggregation
    to_merge = []
    for new_k_name, k_range in k_aggregates.items():
        vt_fft_sel = data_fft.sel(k=k_range)
        if "k" in vt_fft_sel.dims:
            vt_fft_sel = vt_fft_sel.sum("k")
        to_merge.append(vt_fft_sel.expand_dims("k").assign_coords(k=[new_k_name]))
    data_fft_aggr_k = xr.concat(to_merge, dim="k")

    return data_fft_aggr_k
