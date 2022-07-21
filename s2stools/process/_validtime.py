from .. import utils


def add_validtime(da):
    """
    Given a DataArray/ Dataset with dimensions ('reftime', 'hc_year', 'leadtime'), add a coordinate validtime that
    indicates the target date of the forecast. Example: reftime="2000-01-01", hc_year=-1, leadtime=+3D corresponds
    to validtime "1999-01-03".

    Parameters
    ----------
    da : xr.DataArray or xr.Dataset
        Input data, requires dimensions ('reftime', 'hc_year', 'leadtime').

    Returns
    -------
    xr.DataArray or xr.Dataset
        Same dataset as input, but with coordinate validtime.

    Notes
    -------
    Validtime is of type `np.datetime64` and it will not be a dimension.

    Warnings
    _______
    Only dimension `leadtime` is supported, not `days_since_init`.
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
