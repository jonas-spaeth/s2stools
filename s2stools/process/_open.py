import xarray as xr
import numpy as np
from s2stools import utils
import glob
import pandas as pd


def open_files(cf=None, pf=None, chc=None, phc=None, path_pattern=None):
    """
    Open S2S Files and create dimensions reftime and hc_year.

    Parameters
    ----------
    cf : str
        Ending of control forecast files.
    pf : str
        Ending of perturbed forecast files.
    chc : str
        Ending of control hindcast files.
    phc : str
        Ending of perturbed hindcast files.
    path_pattern : str
        Path using glob notaation

    Returns
    -------
    xr.Dataset

    Notes
    -----
    In the near future a new function will enable opening files with 'xr.open_mfdataset' and a preprocess function.

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



def s2sparser(ds):
    """
    Use in the following form: xr.open_mfdataset("/some/path/filename_2017*.nc", preprocess=s2stools.process.s2sparser)
    Will create dimensions reftime, hc_year, leadtime.
    Coordinate validtime is automatically added.
    Watch out: Reftime needs to be somehow encoded in filename and embraced by underscores. Valid filenames: s2s_ecmwf_2020-01-01_cf.nc, s2s_ecmwf_20200101_cf.nc; Invalid filenames: s2s-ecmwf-20200101-cf.nc, s2s_ecmwf_ref20200101_cf.nc, s2s_ecmwf_20200101.nc.
    """
    ### get reftime (needed for relative hindcast year)
    inferred_reftime = _infer_reftime_from_filename(ds.encoding["source"])

    ### control or perturbed forecast?

    if "number" in ds.dims:
        pass
    else:
        ds = ds.assign_coords(number=[0])

    ### realtime forecast or hindcast?

    max_timesteps_of_one_forecast = 100
    dstime = ds.time
    
    is_realtime = len(dstime) < max_timesteps_of_one_forecast
    
    if is_realtime:
        # --> realtime
        reftime = inferred_reftime or ds.time[0].values
        # determine leadtime
        leadtime = (ds.time.values - reftime).astype("timedelta64")
        ds = ds.assign_coords(leadtime=("time", leadtime)).swap_dims(time="leadtime")
        ds = ds.rename(time="validtime")
        ds = ds.assign_coords(hc_year=[0])
    else:
        # --> hindcast
        delta_t = np.diff(ds.time).astype("timedelta64[D]")
        min_days_between_hc_years = 10
        idx_large_delta_t = (
            np.argwhere(
                delta_t > np.timedelta64(min_days_between_hc_years, "D")
            ).ravel()
            + 1
        )
        reftime = inferred_reftime or ds.time.isel(time=idx_large_delta_t[-1]).values

        # determine leadtime
        time_of_first_fc = ds.time.isel(time=slice(0, idx_large_delta_t[0]))
        nt_onefc = len(time_of_first_fc)
        leadtime = (time_of_first_fc - ds.time[0]).values.astype("timedelta64[D]")

        # reshape time and determine hindcast years
        n_hcy = len(ds.time) // nt_onefc
        assert nt_onefc * n_hcy == len(ds.time), "doesnt match"
        reshaped_time = ds.time.values.reshape(
            n_hcy, nt_onefc
        )  # shape: hc_year, leadtime
        # actual hindcast year
        hcy = pd.DatetimeIndex(reshaped_time[:, 0]).year

        # hindcast year relative to reftime
        relative_hcy = hcy - pd.Timestamp(reftime).year

        leadtime_broadcasted = np.broadcast_to(leadtime, reshaped_time.shape).ravel()
        hcy_broadcasted = np.broadcast_to(relative_hcy, reshaped_time.T.shape).ravel(order="F")

        ds = ds.assign_coords(
            leadtime=("time", leadtime_broadcasted),
            hc_year=("time", hcy_broadcasted),
        )

        ds = ds.set_index(time=["leadtime", "hc_year"])
        ds = ds.assign_coords(validtime=("time", dstime.values))
        ds = ds.unstack()
    ds = ds.assign_coords(reftime=[reftime])
    return ds


def _infer_reftime_from_filename(filepath):
    # split filepath to get filename
    filename = filepath.split("/")[-1]
    # split underscores, one item is probability the date
    filename_underscore_splitted = filename.split("_")
    # usually the second to last item is the date, therefore roll to save time
    filename_underscore_splitted = (
        filename_underscore_splitted[-2:] + filename_underscore_splitted[:-2]
    )
    # try to parse reftime from one of these items
    for item in filename_underscore_splitted:
        try:
            inferred_reftime = pd.Timestamp(item)
            inferred_reftime = np.datetime64(inferred_reftime)
            break
        except ValueError:
            continue
        except:
            print("unknown error")
    else:
        inferred_reftime = None
        print(
            "unable to identify correct reftime!"
            + f"Infering reftime from one of these items: {filename_underscore_splitted} failed."
            + "Make sure that the reftime appears in the filename, otherwise can't infer correct reftime."
            + "I mean, we could specify the reftime, e.g., as a list, but I'm not sure if that's convenient... let me know if yes."
        )
    return inferred_reftime