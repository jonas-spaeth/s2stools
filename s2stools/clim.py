import numpy as np
import xarray as xr
import xarray.core.groupby
from tqdm.autonotebook import tqdm
from warnings import warn


def climatology(
    data: xr.DataArray | xr.Dataset,
    window_size: int = 15,
    mean_or_std: str = "mean",
    ndays_clim_filter: int = 7,
    hide_warnings: bool = False,
    groupby: str = "leadtime",
    dim_number_non_exist: bool = False,
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
        e.g. for UKMO it can happen that one is interested in the climatology for 2020-11-01, but hindcasts are available
        only at 2020-11-05; by default, the climatology is computed for 2020-11-05 onwards and the missing previous 4
        days are linearly extrapolated; if this happens, hide_warnings=False will warn you!
    groupby : str
        must be 'leadtime' or 'validtime'

        If "leadtime", create climatology along same leadtime, e.g., different forecasts but always for
        leaditme=+4 days, leadtime=+5 days, ...; If "validtime", create climatology along same day-of-year, e.g.,
        different forecasts but always for March 03, March 04, March 05. Defaults to "leadtime".

    dim_number_non_exist : bool
        Depracated, because it is now automatically checked whether adding dimension "number" is neceesary.
        If True, an ensemble member dimension "number" is added, because it is required for deseasonalization.
        Defaults to False.

    Returns
    -------
    climatology: xr.DataArray or xr.Dataset
        climatological mean or climatological std based on hindcasts

    Warnings
    --------
    If reftimes +- time window are not available then anomalies are still computed, but climatology is less robust.

    Warnings
    --------
    In latest version, groupby="validtime" is not yet implemented to work with the new xarray version.
    Therefore, default changed to groupby="leadtime".

    Notes
    -----
    There my be use cases where no running mean should be used, e.g., for computing anomalies of
    forecast variance, which grows non-linearly!
    Moreover, if anomalies of variance are computed, make sure that groupby is set to "leadtime", because
    spread is more sensitive to the leadtime of course than to the day of year.

    """

    # "number" is a required dimension, if not existent, add it
    if "number" not in data.dims:
        dim_number_non_exist = True
    if dim_number_non_exist:
        data = data.expand_dims(dim=dict(number=[0]))

    # hc_year=0 required, because it is dropped later on using .drop_sel(hc_year=0)
    if 0 not in data.hc_year:
        print(
            "Data seems to include no realtime forecasts. No problem, just to let you know."
        )
        hc_year = data.hc_year.values.tolist()
        data = data.reindex(hc_year=hc_year + [0])

    # Iterate reftimes
    if len(data.reftime) < 0:
        raise ValueError("data must have at least one reftime")
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
        data_for_clim_of_this_reftime = data.sel(
            reftime=hc_reftimes_in_window, hc_year=(data.hc_year != 0)
        )

        # compute climatology
        if groupby == "validtime":
            raise NotImplementedError(
                "s2stools.clim.climatology was modified from v0.3.7 on, \
                because it was not compatible with a recent version of xarray."
                'groupby="validtime" is not yet implemented to work with that new xarray version.'
                'If you really need groupby="validtime",\
                    consider using an older version of s2stools and xarray.'
            )
        elif groupby == "leadtime":
            # perform mean or std aggregation
            if mean_or_std == "mean":
                clim_aggregeated = data_for_clim_of_this_reftime.mean(
                    ["hc_year", "number"]
                ).mean("reftime")
            elif mean_or_std == "std":
                clim_aggregeated = data_for_clim_of_this_reftime.std(
                    ["hc_year", "number"]
                ).mean("reftime")
            else:
                raise ValueError(
                    "mean_or_std must be either 'mean' or 'std', but is {}".format(
                        mean_or_std
                    )
                )

            clim = (
                clim_aggregeated.rolling(
                    leadtime=ndays_clim_filter,
                    center=True,
                    min_periods=1,
                ).mean()  # rolling mean
            ).expand_dims(dim=dict(reftime=[reftime.astype("datetime64[ns]")]))

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


def deseasonalize(
    data,
    window_size=15,
    standardize=False,
    ndays_clim_filter=7,
    hide_print=True,
    hide_plot=True,
    hide_warnings=False,
    return_clim_lists=False,
    dim_number_non_exist=False,
):
    """
    Compute anomalies from the climatological mean. Deseasonalization is based on hindcasts.

    Parameters
    ----------
    data : xr.Dataset or xr.DataArray
        The raw data.
    window_size : int
        The mean is constructed using all reftimes within this plus-minus-day-interval.
    standardize : bool
        If True, compute standardized anomalies.
    ndays_clim_filter : int
        Apply running mean to the climatology.
    hide_print : bool
        If False, print how many reftimes are used for each climatology. Defaults to True.
    hide_plot : bool
        If False, plot the climatology.
    hide_warnings : bool
        ???
    return_clim_lists : bool
        If True, also return the constructed climatologies. If False, only return anomalies.
    dim_number_non_exist : bool
        If True, an ensemble member dimension "number" is added, because it is required for deseasonalization.
        Defaults to False.

    Returns
    -------
    xr.Dataset or xr.DataArray
        Anomalies If return_clim_lists=True, return tuple anom, clim_list, climstd_list.

    Warnings
    --------
    .. deprecated:: 0.3.0
          ``deseasonalize`` will be removed in the future, it is replaced by
          ``climatology``

    """
    warn(
        "Better use s2stools.clim.climatology instead of s2stools.clim.deseasonalize and then compute anomalies afterwards.",
        category=DeprecationWarning,
    )

    data_by_reftime_list = []
    clim_list = []
    climstd_list = []
    with xr.set_options(use_flox=False):
        for reftime in data.reftime:
            reftime = reftime.values.astype("datetime64[D]")
            window = np.arange(reftime - window_size, reftime + window_size)
            # hc_reftimes_in_window = (
            #     data.sel(reftime=np.in1d(data.reftime.values, window))
            #         .sel(hc_year=-1)
            #         .dropna("reftime", how="all")
            #         .reftime.values
            # )
            hc_reftimes_in_window = (
                data.sel(reftime=data.reftime.isin(window))
                .drop_sel(hc_year=0)
                .isel(hc_year=0)
                .reftime  # .values
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
                            clim_stacked.reftime.values
                            - reftime
                            + clim_stacked.leadtime.values
                        ).astype("timedelta64[D]"),
                    )
                )
                .unstack()
                .groupby("days_since_reftime_init")
                .mean()
                .rolling(
                    days_since_reftime_init=ndays_clim_filter,
                    center=True,
                    min_periods=1,
                )
                .mean()
                .sel(
                    days_since_reftime_init=slice(
                        np.timedelta64(0, "D"), data.leadtime[-1]
                    )
                )
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
                                climstd_stacked.reftime.values.astype("datetime64[D]")
                                - reftime
                                + climstd_stacked.leadtime.values
                            ).astype("timedelta64[D]"),
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
                    .sel(
                        days_since_reftime_init=slice(
                            np.timedelta64(0, "D"), data.leadtime[-1]
                        )
                    )
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
