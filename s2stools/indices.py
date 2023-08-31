import numpy as np
import pandas as pd
import xarray as xr


def download_mjo():
    """
    Download MJO phase and amplitude from Columbia university: "https://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.phase/"

    Returns
    -------
    da_phase, da_phase : [xr.DataArray, xr.DataArray]
        Madden Julian Oscillation Realtime Multivariate Index: Phase, Amplitude
    """
    try:
        import pydap
    except ImportError:
        print("MJO download requires package pydap, which is not installed. Consider: pip install pydap")
        return xr.DataArray(name='mjo_phase'), xr.DataArray(name='mjo_mag')
    else:
        path_amplitude = 'http://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.amplitude/dods'
        da_amplitude = xr.open_dataset(path_amplitude).amplitude.rename('mjo_mag')

        path_phase = 'http://iridl.ldeo.columbia.edu/SOURCES/.BoM/.MJO/.RMM/.phase/dods'
        da_phase = xr.open_dataset(path_phase).phase.rename('mjo_phase')

        def time_coordinates(dataarray):
            julian_days = dataarray["T"].values.astype('int').astype('timedelta64[D]')
            base_date = np.datetime64('-4713-11-24')  # julian calendar
            dates = base_date + julian_days
            return dataarray.rename(T='time').assign_coords(time=dates)

        return [time_coordinates(da) for da in [da_phase, da_amplitude]]


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
    # dateparse = lambda x: datetime.strptime(x, "%y%m")
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
        # date_parser=dateparse,
        date_format="%y%m",
        parse_dates=[0],
    )
    qbo_xr = qbo.to_xarray().to_array('p')
    # assign pressure coordinates and attributes
    # divide by 10 to get m/s instead of dm/s
    data = (qbo_xr / 10).assign_coords(p=[int(str(p[:-3])) for p in qbo_xr.p.values]).rename("u").assign_attrs(
        units="m/s"
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
        merge_list.append(download_qbo())
    return xr.merge(merge_list, join="outer")


def nao() -> xr.DataArray:
    """
    Downloading the North Atlantic Oscillation index provided by NCEP.

    Returns
    -------
    nao_index : xr.DataArray

    References
    -----
    https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.index.b500101.current.ascii
    """
    path = (
        "https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.index.b500101.current.ascii"
    )
    nao_index = pd.read_table(
        path, names=["year", "month", "day", "nao"], delim_whitespace=True
    )
    nao_index = nao_index[~np.isnan(nao_index.nao)]
    nao_index["time"] = pd.to_datetime(nao_index[["year", "month", "day"]])
    nao_index = nao_index.drop(columns=["year", "month", "day"])
    nao_index = nao_index.set_index("time")
    nao_index = nao_index.to_xarray()
    return nao_index
