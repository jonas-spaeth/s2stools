import xarray as xr


def data_statistics(data_comp):
    """compute mean, std, min, max all at once.

    Args:
        data_comp (xr.DataArray): Compsite DataArray for which statistics shall be computed, expected dimensions are "i" and "days_since_event".

    Returns:
        tuple: (mean, std, min, max), all of type xr.DataArray with dimenesion only "days_since_event".
    """
    data_comp_mean = data_comp.mean("i")
    data_comp_std = data_comp.std("i")
    data_comp_min = data_comp.min("i")
    data_comp_max = data_comp.max("i")
    return data_comp_mean, data_comp_std, data_comp_min, data_comp_max


def data_percentiles(data_comp):
    """compute percentiles 10, 30, 70, 90, 98

    Args:
        data_comp (xr.DataArray): Composite DataArray to compute percentiles, expected dimensions are "i" and "days_since_event".

    Returns:
        xr.DataArray: Composite DataArray with dimesions "quantile" and "days_since_event".
    """
    data_comp.load()

    quantiles = [2, 10, 30, 70, 90, 98]
    data_comp_quantiles = {}
    for q in quantiles:
        data_comp_quantiles[str(q)] = data_comp.quantile(q / 100, dim="i")
    return data_comp_quantiles

