from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr


def download_mjo():
    """
    Download MJO RMM index from Columbia university: "https://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.phase/T+exch+table-+text+text+skipanyNaN+-table+.html"

    Returns
    -------
    da : xr.DataArray
        Madden Julian Oscillation Realtime Multivariate Index
    """
    mjo_phase_html = "https://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.phase/T+exch+table-+text+text+skipanyNaN+-table+.html"
    raw = pd.read_html(mjo_phase_html)
    time = pd.to_datetime(raw[0]["Time"]["julian_day"], format="%d %b %Y")
    phase = raw[0]["phase"]["Unnamed: 1_level_1"].rename("mjo_phase")

    mjo_phase = xr.DataArray(phase, coords={"time": time})

    mjo_mag_html = "https://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.amplitude/T+exch+table-+text+text+skipanyNaN+-table+.html"
    raw = pd.read_html(mjo_mag_html)
    time = pd.to_datetime(raw[0]["Time"]["julian_day"], format="%d %b %Y")
    mag = raw[0]["amplitude"]["Unnamed: 1_level_1"].rename("mjo_mag")

    mjo_mag = xr.DataArray(mag, coords={"time": time})

    return mjo_phase, mjo_mag


def download_enso(interp_daily=False) -> xr.Dataset:
    """
    Download ENSO 3.4 index from NOAA: "https://psl.noaa.gov/gcos_wgsp/Timeseries/Data/nino34.long.anom.data"

    Parameters
    ----------
    interp_daily : bool
        data comes at monthly resolution, if True interpolate to daily. Defaults to False.

    Returns
    -------
    da : xr.DataArray
        El Nino Southern Oscillation 3.4 index
    """
    url = "https://psl.noaa.gov/gcos_wgsp/Timeseries/Data/nino34.long.anom.data"

    df = pd.read_table(
        url,
        skiprows=1,
        delim_whitespace=True,
        #    index_col=0,
        names=["year"] + list(np.arange(1, 13)),
        skipfooter=7,
        engine="python",
        na_values=-99.99,
    )
    df2 = df.melt(var_name="month", value_name="enso", id_vars="year")
    df2["time"] = pd.to_datetime(
        df2["year"].astype(str) + "-" + df2["month"].astype("str")
    )
    df2.set_index("time", inplace=True)
    df2.drop(columns=["year", "month"], inplace=True)
    df2.sort_index(inplace=True)
    enso = df2.squeeze()
    enso_xr = enso.to_xarray()

    if interp_daily:
        enso_xr = enso_xr.resample(time="1d").interpolate(kind="linear").dropna("time")
    return enso_xr


def get_qbo():
    """
    Backward compatibility for download_qbo

    Returns
    -------
    ds : xr.DataArray
        Quasi-Biennial Oscillation

    Warnings
    --------
    .. deprecated:: 0.3.0
        Use :func:`download_qbo()` for consistency with mjo, enso

    """
    return download_qbo()


def download_qbo():
    """
    Download Quasi-Biennial-Oscillation wind data from FU Berlin: "https://www.geo.fu-berlin.de/met/ag/strat/produkte/qbo/qbo.dat"

    Returns
    -------
    ds : xr.DataArray
        Quasi-Biennial Oscillation

    See Also
    --------
    :func:`download_mjo`, :func:`download_enso`
    """
    path = "https://www.geo.fu-berlin.de/met/ag/strat/produkte/qbo/qbo.dat"
    dateparse = lambda x: datetime.strptime(x, "%y%m")
    qbo = pd.read_table(
        path,
        skiprows=381,
        delim_whitespace=True,
        index_col=0,
        usecols=range(1, 9),
        names=[
            "IIIII",
            "time",
            "70hPa",
            "50hPa",
            "40hPa",
            "30hPa",
            "20hPa",
            "15hPa",
            "10hPa",
        ],
        date_parser=dateparse,
        parse_dates=[0],
    )
    qbo_xr = qbo.to_xarray()
    data = (
        xr.concat([qbo_xr[v] / 10 for v in qbo_xr.data_vars], dim="p")
        .assign_coords(p=[int(n[:2]) for n in list(qbo_xr.data_vars)])
        .rename("u")
        .assign_attrs(units="m/s")
    )
    data["p"] = data.p.assign_attrs(units="hPa", long_name="pressure level")

    return data


def download_indices(enso=True, mjo=True, u60=False, qbo=True):
    """
    Download a couple of teleconnection indices.

    Parameters
    ----------
    enso : bool
    mjo : bool
    u60 : bool
    qbo : bool

    Returns
    -------
    ds : xr.Dataset

    See Also
    --------
    :func:`download_enso`, :func:`download_mjo`, :func:`download_mjo`
    """
    merge_list = []
    if enso:
        merge_list.append(download_enso(interp_daily=True))
    if mjo:
        merge_list += list(download_mjo())
    if u60:
        raise NotImplemented
        # merge_list.append(load_u60_10hpa())
    if qbo:
        merge_list += list(download_qbo())
        # merge_list.append(load_qbo_pcs())
    return xr.merge(merge_list, join="outer")
