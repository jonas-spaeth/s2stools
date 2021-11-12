import xarray as xr
from ecmwfapi import ECMWFDataServer


target = "/project/meteo/work/Jonas.Spaeth/gitprojects/stos/data"

def test(**kwargs):   

    server = ECMWFDataServer()
    print()
    server.retrieve({**{
        "class": "s2",
        "dataset": "s2s",
    }, **kwargs}
    )