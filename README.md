# stos

Python tools for working with S2S forecast data

##### workflow later

```python

# download and store data
s2s.download.retreive(parameter=[u, v], level=[850, 500], fc_type=["rt", "hc"], area_nesw_box=[90, 180, 0, -180])

# open with inittime, hc_yr, leadtime dimensions, and optionally validtime variable
ds = s2s.process.open_s2s(path, transform=True)

# desesonalize
clim = s2s.clim.deseasonalize(ds)
anom = s2s.clim.anom_from_clim(ds, clim)

# event composite
eventlist = stos.events.find_events(ds, algorithm)
ds_comp = stos.events.composite_from_eventlist(eventlist)

# plotting
s2s.plot.spaghetti(ds)
````