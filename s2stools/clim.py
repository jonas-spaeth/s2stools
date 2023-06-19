import numpy as np
import xarray as xr
import xarray.core.groupby
from tqdm.autonotebook import tqdm


def climatology(
        data,
        window_size=15,
        mean_or_std="mean",
        ndays_clim_filter=7,
        hide_warnings=False,
        groupby="validtime",
):
    """
    Compute anomalies from the climatological mean. Deseasonalization is based on hindcasts.

    Parameters
    ----------
    data : xr.Dataset or xr.DataArray
        The raw data.
    window_size : int
        The mean is constructed using all reftimes within this plus-minus-day-interval.
    mean_or_std : str
        either 'mean' or 'std'
    ndays_clim_filter : int
        Apply running mean to the climatology.
    hide_warnings : boolean
        ???
    groupby : str
        must be 'leadtime' or 'validtime'

    Returns
    -------
    xr.Dataset or xr.DataArray
        xr.DataArray | xr.Dataset

    Warnings
    --------
    If reftimes +- time window are not available then anomalies are still computed, but climatology is less robust.

    Notes
    -----
    There my be use cases where only one reftime and no running mean should be used, e.g., for computing anomalies of
    forecast variance.

    """
    if mean_or_std == "mean":
        aggregation_func = xarray.core.groupby.DataArrayGroupByAggregations.mean
        aggregation_dim = "stacked_reftime_leadtime"
    elif mean_or_std == "std":
        aggregation_func = xarray.core.groupby.DataArrayGroupByAggregations.std
        aggregation_dim = ["stacked_reftime_leadtime", "hc_year"]
    else:
        raise ValueError(f"mean_or_std must be 'mean' or 'std', not '{mean_or_std}'")

    clim_list = []

    for reftime in tqdm(data.reftime, desc="iterating reftimes"):

        # collect all reftimes within time window
        reftime = reftime.values.astype("datetime64[D]")

        window = np.arange(reftime - window_size, reftime + window_size)

        hc_reftimes_in_window = (
            data.sel(reftime=data.reftime.isin(window))
            .drop_sel(hc_year=0)
            .isel(hc_year=0)
            .reftime
        )

        # stacked forecasts from hindcast years
        clim_stacked = data.sel(
            reftime=hc_reftimes_in_window, hc_year=(data.hc_year != 0)
        ).stack(date=("reftime", "leadtime"))
        if mean_or_std == "mean":
            clim_stacked = clim_stacked.mean(["hc_year", "number"])
        elif mean_or_std == "std":
            clim_stacked = clim_stacked.sel(number=0)

        # compute climatology
        if groupby == "validtime":
            clim = (
                (
                    clim_stacked.assign_coords(
                        days_since_reftime_init=(
                            "date",
                            (
                                    clim_stacked.reftime.values
                                    - reftime
                                    + clim_stacked.leadtime.values
                            ).astype("timedelta64[D]"),
                        )
                    )  # days_since_reftime_init measures timedelta relative to initialization
                    .unstack()
                    .groupby("days_since_reftime_init")
                    .apply(
                        aggregation_func, dim=aggregation_dim
                    )  # average or standardize all days that have validtime, e.g., reftime + 2D
                    .rolling(
                        days_since_reftime_init=ndays_clim_filter,
                        center=True,
                        min_periods=1,
                    )
                    .mean()  # rolling mean
                    .sel(
                        days_since_reftime_init=slice(
                            np.timedelta64(0, "D"), data.leadtime[-1]
                        )
                    )  # cut climatology to length of forecast
                    .rename(days_since_reftime_init="leadtime")
                )
                .assign_coords(reftime=reftime)
                .expand_dims("reftime")
            )

        ### remove
        # compute climatology 2
        if groupby == "leadtime":
            if mean_or_std == "mean":
                aggregation_dim = "reftime"
            elif mean_or_std == "std":
                aggregation_dim = ["reftime", "hc_year"]

            clim = (
                (
                    clim_stacked.unstack()
                    .groupby("leadtime")
                    .apply(
                        aggregation_func, dim=aggregation_dim
                    )  # average or standardize all days that have validtime, e.g., reftime + 2D
                    .rolling(
                        leadtime=ndays_clim_filter,
                        center=True,
                        min_periods=1,
                    )
                    .mean()  # rolling mean
                    .sel(
                        leadtime=slice(np.timedelta64(0, "D"), data.leadtime[-1])
                    )  # cut climatology to length of forecast
                )
                .assign_coords(reftime=reftime)
                .expand_dims("reftime")
            )
        ###

        if len(clim.leadtime) != len(data.leadtime):
            if not hide_warnings:
                print(
                    "WARNING: no climatology available for {} days of reftime {}. Applying interpolation.".format(
                        len(data.leadtime) - len(clim.leadtime), reftime
                    )
                )
            clim = clim.interp(
                leadtime=data.leadtime,
                kwargs={"fill_value": "extrapolate"},
            )

        clim_list.append(clim)

    clim_list = xr.concat(clim_list, dim="reftime")
    return clim_list


def nam(dataarray):
    """
    Not implemented.

    Parameters
    ----------
    dataarray : xr.DataArray

    Returns
    -------

    """
    print("Not Implemented")
