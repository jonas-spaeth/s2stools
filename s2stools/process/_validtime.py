from .. import utils


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
