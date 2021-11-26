````python
import xarray as xr
import events

ds = xr.Dataset(...)

event_list = events.find_ssw(ds.u60)
event_dict = events.events_list_to_dict(event_list) 
events.events_dict_to_json(event_dict, path="myPath") # maybe better export to utils

events.composite_from_json("myPath", data=ds)
````

Desired Improvements:
* `composite_from_json()` should not need `days_since_init` 
* change from `days_since_init` to `leadtime`
