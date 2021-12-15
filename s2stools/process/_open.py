import xarray as xr
import numpy as np
from s2stools import utils
import glob


def open_files(cf=None, pf=None, chc=None, phc=None, path_pattern=None, max_lt=46):
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
        cf = path_pattern + (cf or "_rt_cf.nc")
        pf = path_pattern + (pf or "_rt_pf.nc")
        chc = path_pattern + (chc or "_hc_cf.nc")
        phc = path_pattern + (phc or "_hc_pf.nc")

    # obtain lead time
    valid_rt = cf or pf
    if valid_rt is None:
        raise NotImplementedError("Without reading realtime forecasts, cannot infer leadtime.")
    print("realtime path: ", valid_rt)
    onefile_path = glob.glob(valid_rt)[0]
    onefile_ds = xr.open_dataset(onefile_path)
    lt = (onefile_ds.time.values - onefile_ds.time.values[0]).astype("timedelta64[h]")
    if (lt.astype("int") % 24).max() == 0:
        # lead time is given in daily resolution
        lt = lt.astype("timedelta64[D]")

    def preprocess(x, lt):
        """
        -> Todo
        Args:
            x ():
            lt (int): lead time

        Returns:

        """
        max_lt = lt.max()  # maximum leadtime
        n_lt = len(lt)  # n leadtime steps
        lt_ts = lt[1] - lt[0]  # leadtime timestep
        hindcast = True if len(x.time) > n_lt else False
        reftime = utils.add_years(x.time[0].values, 20) if hindcast else x.time[0].values
        x = x.assign_coords(reftime=reftime)

        bins = np.append(x.time[::n_lt], x.time[-1].values + lt_ts)
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
                    .assign_coords(time=lt)  # np.arange(max_lt+1)
                    .rename(time="leadtime")
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
            preprocess=lambda x: preprocess(x, lt=lt),
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
            preprocess=lambda x: preprocess(x, lt=lt),
            combine="nested",
        )
        rt.append(s2s_pf)
        print("")
    if chc:
        print("chc\t", end=" ")
        s2s_chc = xr.open_mfdataset(
            chc,
            concat_dim="reftime",
            preprocess=lambda x: preprocess(x, lt=lt),
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
            preprocess=lambda x: preprocess(x, lt=lt),
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
    return xr.concat(res, dim="hc_year").sortby("hc_year")
