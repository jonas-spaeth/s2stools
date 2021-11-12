from download import *

target = "/project/meteo/work/Jonas.Spaeth/gitprojects/stos/data/t2.nc"

test(**{
    "date": '2021-02-04',
    "expver": "prod",
    "levelist": "1000",
    "levtype": "pl",
    "model": "glob",
    "origin": "ecmf",
    "param": "130",
    "step": "0/24",
    "stream": "enfo",
    "time": "00:00:00",
    "area": ["65", "-10", "50", "130"],
    "type": "cf",
    "format": "netcdf",
    "grid": "2.5/2.5",
    "target": target,
}
)


##### workflow later

# download and store data
s2s.download.retreive(parameter=[u, v], level=[850, 500], fc_type=["rt", "hc"], area_nesw_box=[90, 180, 0, -180])

# open with inittime, hc_yr, leadtime dimensions, and optionally validtime variable
ds = s2s.dim.open_s2s(path, transform=True)

# desesonalize
clim = s2s.clim.deseasonalize(ds)
anom = s2s.clim.anom_from_clim(ds, clim)

# event composite
eventlist = stos.events.find_events(ds, algorithm)
ds_comp = stos.events.composite_from_eventlist(eventlist)

# plotting
s2s.plot.spaghetti(ds)