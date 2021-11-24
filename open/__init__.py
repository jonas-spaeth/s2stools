import xarray as xr
import numpy as np
import utils

def openfiles(path_cf, path_pf, path_chc, path_phc, reftime_dates, names):
    """

    Args:
        path_cf (): list of *-formatted path of control forecasts, for each variable,
    e.g. ['s2s_u60_10hPa_201*_cf.nc', 's2s_z1000_*_cf.nc']
        path_pf (): list of *-formatted path of perturbed forecasts
        path_phc (): list of *-formatted path of control hindcast
        path_chc (): list of *-formatted path of perturbed hindcast
        reftime_dates ():
        names ():

    """

    # variable names
    old_names, new_names = names

    # REAL TIME FC

    # control forecast
    data_cf = xr.merge(
        [xr.open_mfdataset(cf, combine='nested', concat_dim='reftime').sortby('latitude').rename({old_n: new_n}) for
         cf, old_n, new_n in zip(path_cf, old_names, new_names)])
    data_cf = xr.concat([data_cf], dim='number')
    data_cf = data_cf.assign_coords(reftime=[np.datetime64(d) for d in reftime_dates], number=[0])

    # perturbed forecast
    data_pf = xr.merge(
        [xr.open_mfdataset(pf, combine='nested', concat_dim='reftime').sortby('latitude').rename({old_n: new_n}) for
         pf, old_n, new_n in zip(path_pf, old_names, new_names)])
    data_pf = data_pf.assign_coords(reftime=[np.datetime64(d) for d in reftime_dates])

    # combine control and perturbed RealTime forecast (control forecast is number 0)
    data_rt = xr.concat([data_cf, data_pf], dim='number')

    # HINDCASTS

    # control hindcast
    data_chc = xr.merge(
        [xr.open_mfdataset(chc, combine='nested', concat_dim='reftime').sortby('latitude').rename({old_n: new_n})
         for chc, old_n, new_n in zip(path_chc, old_names, new_names)])
    data_chc = xr.concat([data_chc], dim='number')
    data_chc = data_chc.assign_coords(reftime=[np.datetime64(d) for d in reftime_dates], number=[0])

    # perturbed hindcast
    data_phc = xr.merge(
        [xr.open_mfdataset(phc, combine='nested', concat_dim='reftime').sortby('latitude').rename({old_n: new_n})
         for phc, old_n, new_n in zip(path_phc, old_names, new_names)])
    data_phc = data_phc.assign_coords(reftime=[np.datetime64(d) for d in reftime_dates])

    # combine control and perturbed HindCast (control forecast is number 0)
    data_hc = xr.concat([data_chc, data_phc], dim='number')

    # COMBINE REALTIME AND HINDCAST

    data = xr.concat([data_rt, data_hc], dim='fc_type').assign_coords(fc_type=['realtime', 'hindcast'])

    return data


def process_s2s(cf=None, pf=None, chc=None, phc=None, path_pattern=None, max_lt=46):
    """
    TODO: Can we autodetect max_lt?
    TODO: Compare to process_s2s_ukmo – possible to generalize?
    Args:
        cf ():
        pf ():
        chc ():
        phc ():
        path_pattern ():
        max_lt ():

    Returns:

    """
    if path_pattern:
        cf = path_pattern + "_rt_cf.nc"
        pf = path_pattern + "_rt_pf.nc"
        chc = path_pattern + "_hc_cf.nc"
        phc = path_pattern + "_hc_pf.nc"

    def preprocess(x, max_lt):
        """
        -> Todo
        Args:
            x ():
            max_lt (int): maximum lead time

        Returns:

        """
        hindcast = True if len(x.time) > max_lt+1 else False
        reftime = utils.add_years(x.time[0].values, 20) if hindcast else x.time[0].values

        x = x.assign_coords(reftime=reftime)

        bins = np.append(x.time[::max_lt+1], x.time[-1].values.astype("datetime64[D]") + 1)
        bin_labels = np.arange(-20, 0) if hindcast else np.array([0])
        groups = x.groupby_bins(
            "time", bins=bins, labels=bin_labels, include_lowest=True, right=False
        )
        hcyear_collection = [g[1].assign_coords(hc_year=int(g[0])) for g in groups]

        res = []
        for hcy in hcyear_collection:
            fc_start = utils.add_years(reftime, int(hcy.hc_year))
            hcy_timeslice = (
                hcy.sel(time=slice(fc_start, fc_start + max_lt))
                    .assign_coords(time=np.arange(max_lt+1))
                    .rename(time="days_since_init")
            )
            res.append(hcy_timeslice)
        x = xr.concat(res, dim="hc_year")

        return x

    rt = []
    hc = []

    if cf:
        print("cf\t", end=" ")
        s2s_cf = xr.open_mfdataset(
            cf,
            concat_dim="reftime",
            preprocess=lambda x: preprocess(x, max_lt=max_lt),
            combine="nested",
        )
        s2s_cf = s2s_cf.assign_coords(number=0).expand_dims("number")
        rt.append(s2s_cf)
        print("")
    if pf:
        print("pf\t", end=" ")
        s2s_pf = xr.open_mfdataset(
            pf,
            concat_dim="reftime",
            preprocess=lambda x: preprocess(x, max_lt=max_lt),
            combine="nested",
        )
        rt.append(s2s_pf)
        print("")
    if chc:
        print("chc\t", end=" ")
        s2s_chc = xr.open_mfdataset(
            chc,
            concat_dim="reftime",
            preprocess=lambda x: preprocess(x, max_lt=max_lt),
            combine="nested",
        )
        s2s_chc = s2s_chc.assign_coords(number=0).expand_dims("number")
        hc.append(s2s_chc)
        print("")
    if phc:
        print("phc\t", end=" ")
        s2s_phc = xr.open_mfdataset(
            phc,
            concat_dim="reftime",
            preprocess=lambda x: preprocess(x, max_lt=max_lt),
            combine="nested",
        )
        hc.append(s2s_phc)
        print("")

    res = []
    if len(rt) != 0:
        rt_merged = xr.concat(rt, dim="number")
        res.append(rt_merged)
    if len(hc) != 0:
        hc_merged = xr.concat(hc, dim="number")
        res.append(hc_merged)
    return xr.concat(res, dim="hc_year")