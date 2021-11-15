import numpy as np
import datetime
import pandas as pd
from download import S2SDownloader
import utils


class S2SDownloaderECMWF(S2SDownloader):
    DEFAULT_REQUEST_ECMWF = {
        "origin": "ecmf"
    }

    FC_STEP = "0/24/48/72/96/120/144/168/192/216/240/264/288/312/336/360/384/408/432/456/480/504/528/552/576/600/624/648/672/696/720/744/768/792/816/840/864/888/912/936/960/984/1008/1032/1056/1080/1104"

    request = {}

    def __init__(self):
        S2SDownloader.__init__(self)
        self.request = super().DEFAULT_REQUEST
        self.request.update(self.DEFAULT_REQUEST_ECMWF)

    def retreive(self, param, reftime, plevs, target, area=None, step="all", exact_reftime=False, **kwargs):

        self.request["param"] = utils.list_to_string(param)
        self.request["levelist"] = utils.list_to_string(list(plevs))
        self.request["target"] = target

        # all forecast steps
        if step == "all":
            step = self.FC_STEP
        else:
            step = utils.list_to_string(step)
        self.request["step"] = step

        # drop dates without model initialzation
        filtered_reftime = self.filter_reftimes(reftime)
        if exact_reftime:
            assert filtered_reftime == reftime, "reftime contains dates that are no reftimes. only allowed if assert_reftime=False."
        self.request["date"] = utils.list_to_string(list(filtered_reftime))

        self.request.update(dict(**kwargs))

        # call parent retrieve
        super().retrieve(self.request)

    @staticmethod
    def filter_reftimes(dates):
        dates = np.atleast_1d(dates)
        dates_pd = pd.Series(dates)
        filtered_dates = dates[(dates_pd.dt.weekday == 0) | (dates_pd.dt.weekday == 3)]
        assert len(filtered_dates) > 0, "no dates left after filtering reftimes"
        return filtered_dates