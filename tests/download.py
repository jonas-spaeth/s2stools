import xarray as xr
from ecmwfapi import ECMWFDataServer
from pprint import pprint

target = "/project/meteo/work/Jonas.Spaeth/gitprojects/stos/data"

def retreive(reftime, plevs, **kwargs):
    """[summary]

    Args:
        reftime (list of np.datetime64["D"]): init dates
        plevs (int or list of int): pressure levels
    """    



    target = "/project/meteo/work/Jonas.Spaeth/gitprojects/stos/data/t2.nc"
    
    plevs = list_to_string(list(plevs))
    date = filter_s2s_init_dates(reftime)

    request = {
        "class": "s2",
        "dataset": "s2s",
        "expver": "prod",
        "date": '2021-02-04',
        "expver": "prod",
        "levelist": plevs,
        "levtype": "pl",
        "model": "glob",
        "origin": "ecmf",
        "param": "u/v",
        "step": "0/24",
        "stream": "enfo",
        "time": "00:00:00",
        "area": ["65", "-10", "50", "130"],
        "type": "cf",
        "format": "netcdf",
        "grid": "2.5/2.5",
        "target": target,
    }
    request.update(**kwargs)


    print(request)

    server = ECMWFDataServer()
    
    pprint(request)

    server.retrieve(
        request
    )

# utils
def list_to_string(l):
    return "/".join([str(i) for i in l])

def filter_s2s_init_dates(d):
    # TODO: implement
    return d