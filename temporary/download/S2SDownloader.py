from ecmwfapi import ECMWFDataServer


class S2SDownloader:
    DEFAULT_REQUEST = {
        "class": "s2",
        "dataset": "s2s",
        "expver": "prod",
        # "date": '2021-02-04',
        "expver": "prod",
        # "levelist": plevs,
        "levtype": "pl",
        "model": "glob",
        # "origin": "ecmf",
        # "step": "0/24",
        # "stream": "enfo",
        "time": "00:00:00",
        # "area": ["65", "-10", "50", "130"],
        "type": "cf",
        "format": "netcdf",
        # "grid": "2.5/2.5",
        # "target": target,
    }

    request = {}

    def __init__(self):
        pass

    def retrieve(self, request):
        print("Target: ", request["target"])

        if True:
            server = ECMWFDataServer()

            server.retrieve(
                request
            )
