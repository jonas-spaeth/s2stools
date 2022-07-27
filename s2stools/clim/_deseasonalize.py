import numpy as np
import xarray as xr


def deseasonalize(
        data,
        window_size=15,
        standardize=False,
        ndays_clim_filter=7,
        hide_print=True,
        hide_plot=True,
        hide_warnings=False,
        return_clim_lists=False,
):
    """
    Compute anomalies from the climatological mean. Deseasonalization is based on hindcasts.

    Parameters
    ----------
    data : xr.Dataset or xr.DataArray
        The raw data.
    window_size : int
        The mean is constructed using all reftimes within this plus-minus-day-interval.
    standardize : boolean
        If True, compute standardized anomalies.
    ndays_clim_filter : int
        Apply running mean to the climatology.
    hide_print : boolean
        If False, print how many reftimes are used for each climatology. Defaults to True.
    hide_plot : boolean
        If False, plot the climatology.
    hide_warnings : boolean
        ???
    return_clim_lists : boolean
        If True, also return the constructed climatologies. If False, only return anomalies.

    Returns
    -------
    xr.Dataset or xr.DataArray
        Anomalies If return_clim_lists=True, return tuple anom, clim_list, climstd_list.

    Warnings
    --------
    If reftimes +- time window are not available then anomalies are still computed, but climatology is less robust.

    Notes
    -----
    There my be use cases where only one reftime and no running mean should be used, e.g., for computing anomalies of
    forecast variance.

    """
    data_by_reftime_list = []
    clim_list = []
    climstd_list = []

    for reftime in data.reftime:
        reftime = reftime.values.astype("datetime64[D]")
        window = np.arange(reftime - window_size, reftime + window_size)
        hc_reftimes_in_window = (
            data.sel(reftime=np.in1d(data.reftime.values, window))
                .sel(hc_year=-1)
                .dropna("reftime", how="all")
                .reftime.values
        )

        # ***** COMPUTE CLIMATOLOGY *****
        clim_stacked = (
            data.sel(reftime=hc_reftimes_in_window, hc_year=(data.hc_year != 0))
                .mean(["hc_year", "number"])
                .stack(date=("reftime", "leadtime"))
        )

        clim = (
            clim_stacked.assign_coords(
                days_since_reftime_init=(
                    "date",
                    (
                            clim_stacked.reftime.values - reftime + clim_stacked.leadtime.values
                    ).astype("timedelta64[D]")
                    ,
                )
            )
                .unstack()
                .groupby("days_since_reftime_init")
                .mean()
                .rolling(
                days_since_reftime_init=ndays_clim_filter, center=True, min_periods=1
            )
                .mean()
                .sel(days_since_reftime_init=slice(np.timedelta64(0, "D"), data.leadtime[-1]))
                .rename(days_since_reftime_init="leadtime")
        )
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

        if not hide_print:
            print(
                "climatology for reftime {}: n refs = {}".format(
                    reftime, len(hc_reftimes_in_window)
                )
            )
        if not hide_plot:
            clim.plot(label=reftime)

        if standardize:
            # ***** COMPUTE STD CLIMATOLOGY *****

            climstd_stacked = (
                data.sel(reftime=hc_reftimes_in_window, hc_year=(data.hc_year != 0))
                    .sel(number=0)
                    .stack(date=("reftime", "leadtime"))
            )

            climstd = (
                climstd_stacked.assign_coords(
                    days_since_reftime_init=(
                        "date",
                        (
                                climstd_stacked.reftime.values.astype(
                                    "datetime64[D]") - reftime + climstd_stacked.leadtime.values
                        ).astype("timedelta64[D]")
                        ,
                    )
                )
                    .unstack()
                    .groupby("days_since_reftime_init")
                    .std(["stacked_reftime_leadtime", "hc_year"])
                    .rolling(
                    days_since_reftime_init=ndays_clim_filter,
                    center=True,
                    min_periods=1,
                )
                    .mean()
                    .sel(days_since_reftime_init=slice(np.timedelta64(0, "D"), data.leadtime[-1]))
                    .rename(days_since_reftime_init="leadtime")
            )

            if len(climstd.leadtime) != len(data.leadtime):
                if not hide_warnings:
                    print(
                        "WARNING: no std climatology available for {} days of reftime {}. Applying interpolation.".format(
                            len(data.leadtime) - len(climstd.leadtime),
                            reftime,
                        )
                    )
                climstd = climstd.interp(
                    leadtime=data.leadtime,
                    kwargs={"fill_value": "extrapolate"},
                )

            data_anomstd_oneref = (data.sel(reftime=reftime) - clim) / climstd
            data_by_reftime_list.append(data_anomstd_oneref)
            climstd_list.append(climstd)

        else:
            data_anom_oneref = data.sel(reftime=reftime) - clim
            data_by_reftime_list.append(data_anom_oneref)

        clim_list.append(clim)

    anom = xr.concat(data_by_reftime_list, dim="reftime").transpose(*data.dims)

    if return_clim_lists:
        return anom, clim_list, climstd_list
    else:
        return anom


def nam(z):
    z_sel = z.sortby("latitude").sel(latitude=slice(65, 90))
    weights = np.cos(np.deg2rad(z.latitude))
    avg = (z_sel * weights).mean(["longitude", "latitude"])
    res = -1 * deseasonalize(avg, standardize=True)
    return res
