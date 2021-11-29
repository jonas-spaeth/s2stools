import xarray as xr
import numpy as np
import utils
import glob
import datetime


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
        cf = path_pattern + "_rt_cf.nc" or cf
        pf = path_pattern + "_rt_pf.nc" or pf
        chc = path_pattern + "_hc_cf.nc" or chc
        phc = path_pattern + "_hc_pf.nc" or phc

    # obtain lead time
    valid_rt = cf or pf
    if valid_rt is None:
        raise NotImplementedError("Without reading realtime forecasts, cannot infer leadtime.")
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
        hindcast = True if len(x.time) > max_lt + 1 else False
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


def stack_fc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year", "number")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year", "number"))


def stack_ensfc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year"))


def add_validtime(da):
    """
    number = number
    fc_day = reftime + hc_year + leadtime
    hc_year = hc_year
    lead = leadtime
    """
    da_stacked = da.stack(day=("reftime", "hc_year", "leadtime"))
    fc_day = (
            utils.add_years(da_stacked.reftime.values, da_stacked.hc_year.values)
            + da_stacked.leadtime.values
    )

    fc_day_reshaped = fc_day.reshape(
        len(da.reftime), len(da.hc_year), len(da.leadtime)
    )
    res = da.assign_coords(
        validtime=(("reftime", "hc_year", "leadtime"), fc_day_reshaped)
    )
    return res


"""
def add_validtime(ds):
    ds_stacked = ds.stack(dates=("reftime", "hc_year", "leadtime"))
    dates = get_fc_date(data=s2s_nam1000_stacked)

    era5_nam1000_interp = era5_nam1000.interp(
        time=dates
    ).compute()  # era5_nam.mean("dayofyear").interp(time=dates).compute()

    res = xr.DataArray(
        data=era5_nam1000_interp.nam1000,
        coords=s2s_nam1000_stacked.mean("number").coords,
        dims=s2s_nam1000_stacked.mean("number").dims,
    )

    nam1000 = xr.merge(
        [s2s_nam1000_stacked.rename("nam1000_s2s"), res.rename("nam1000_era5")]
    ).unstack()
    nam1000


 def fc_dates(reftime=None, hc_year=None, leadtime=None, data=None):
    if data is not None:
        reftime, hc_year, leadtime = (
            data.reftime,
            data.hc_year,
            data.leadtime,
        )
    leadtime = np.array(leadtime)
    dates = utils.add_years(reftime, hc_year) + leadtime
    return dates


def get_fc_date(reftime=None, hc_year=None, dsi=None, data=None):
    if data is not None:
        reftime, hc_year, dsi = (
            data.reftime,
            data.hc_year,
            data.leadtime,
        )
    res = []
    reftime = np.atleast_1d(reftime)
    hc_year = np.atleast_1d(hc_year)
    dsi = np.atleast_1d(dsi)

    for x, y, z in zip(reftime, hc_year, dsi):
        z = int(z)
        # add days
        x = x.astype("datetime64[D]") + z
        # convert to timestamp:
        ts = (x - np.datetime64("1970-01-01T00:00:00Z")) / np.timedelta64(1, "s")
        # standard utctime from timestamp
        dt = datetime.utcfromtimestamp(ts)
        # update year
        dt = dt.replace(year=dt.year + y)
        # convert back to numpy.datetime64:
        res.append(np.datetime64(dt))
    return res

##################

s2s_nam1000_stacked = s2s.nam1000.stack(dates=("reftime", "hc_year", "leadtime"))
dates = get_fc_date(data=s2s_nam1000_stacked)

era5_nam1000_interp = era5_nam1000.interp(
    time=dates
).compute()  # era5_nam.mean("dayofyear").interp(time=dates).compute()

res = xr.DataArray(
    data=era5_nam1000_interp.nam1000,
    coords=s2s_nam1000_stacked.mean("number").coords,
    dims=s2s_nam1000_stacked.mean("number").dims,
)

nam1000 = xr.merge(
    [s2s_nam1000_stacked.rename("nam1000_s2s"), res.rename("nam1000_era5")]
).unstack()
nam1000
"""
