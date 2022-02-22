import numpy as np
import datetime
import json
from datetime import datetime
from .. import S2SDownloader
from .model_setup import *

class S2SDownloaderECMWF(S2SDownloader):
    DEFAULT_REQUEST_ECMWF = {
        "origin": "ecmf"
    }
    STEP_ALL = utils.list_to_string(np.arange(0, 1104 + 1, 24))
    MODEL_NAME = "ecmwf"

    request = {}

    def __init__(self):
        S2SDownloader.__init__(self)
        self.request = super().DEFAULT_REQUEST
        self.request.update(self.DEFAULT_REQUEST_ECMWF)

    def retreive(self, param, reftime, plevs, file_descr, path="./", area=None, step="all", exact_reftime=False,
                 write_info_file=True,
                 rt_cf_kwargs={},
                 rt_pf_kwargs={}, hc_cf_kwargs={}, hc_pf_kwargs={}, **kwargs):
        """
        Download S2S ECMWF data.
        Args:
            param (): Parameters to download.
            reftime (): Realtime Dates.
            plevs (): Pressure Levels.
            file_descr (): Description of fields that will show up in the file name.
            path (): Directory to store the output.
            area (str): Area in the format "N/W/S/E"
            step (): Forecast lead times; if "all" then download all available in daily resolution.
            exact_reftime (bool): If true, assert that the given reftime dates are actual model initialization dates, if False, then automatically drop the invalid dates.
            write_info_file (bool): If true, write a .json file into the target directory with info about request.
            rt_cf_kwargs (dict): Additional request keywords passed to realtime control forecasts.
            rt_pf_kwargs (dict): Additional request keywords passed to realtime perturbed forecasts.
            hc_cf_kwargs (dict): Additional request keywords passed to hindcast control forecasts.
            hc_pf_kwargs (dict): Additional request keywords passed to hindcast perturbed forecasts.
            **kwargs (): Additional request keywords.
        """
        self.request["param"] = utils.list_to_string(list(param))
        self.request["levelist"] = utils.list_to_string(list(plevs))
        if area is not None:
            self.request["area"] = area

        # all forecast steps
        if step == "all":
            step = self.STEP_ALL
        else:
            step = utils.list_to_string(step)
        self.request["step"] = step

        # drop dates without model initialization
        filtered_reftime = self.filter_reftimes(reftime)
        if exact_reftime:
            assert filtered_reftime == reftime, "reftime contains dates that are no reftimes. only allowed if " \
                                                "assert_reftime=False. "

        self.request.update(dict(**kwargs))

        for d in filtered_reftime:

            self.request["date"] = str(d)

            for fc_type, fc_type_kwargs, fc_type_class in [("rt_cf", rt_cf_kwargs, RtCf), ("rt_pf", rt_pf_kwargs, RtPf),
                                                           ("hc_cf", hc_cf_kwargs, HcCf),
                                                           ("hc_pf", hc_pf_kwargs, HcPf)]:
                if fc_type_kwargs.get("skip", False):
                    continue
                fc_type_kwargs = dict(fc_type_class(d).request, **fc_type_kwargs)
                self.request["target"] = path + "/" + self.file_name(
                    file_descr=file_descr, fc_type=fc_type, reftime=d
                )
                full_request = dict(self.request, **fc_type_kwargs)
                super().retrieve(full_request)
                if write_info_file:
                    self.make_request_info_file(path, file_descr, fc_type, full_request, filtered_reftime)
            write_info_file = False

    def file_name(self, file_descr, fc_type, reftime):
        """
        File name convention.
        Args:
            file_descr (str): Description of fields that is included in the file name.
            fc_type (str): Usually one of "rt_cf", "rt_pf", "hc_cf", "hc_pf".
            reftime (np.datetime64): Model realtime initilization date (reference time).

        Returns : File Name (str)

        """
        target = "s2s_{model}_{file_descr}_{reftime}_{fc_type}.nc".format(
            model=self.MODEL_NAME,
            file_descr=file_descr,
            fc_type=fc_type,
            reftime=reftime
        )
        return target

    def make_request_info_file(self, path, file_descr, fc_type, full_request, all_dates):
        """
        Create a .json file including information about s2stools request.
        Args:
            path (str): Directory to place file.
            file_descr (str): Description of fields that is included in file name.
            fc_type (str): Usually one of "rt_cf", "rt_pf", "hc_cf", "hc_pf".
            full_request (dict): Full API request.
            all_dates (list of np.datetime64[D]):  List of all requested reftime dates.
        """
        now = datetime.now().isoformat(timespec="minutes")
        filename = "request_s2s_{model_name}_{file_descr}_{datetime}_{fc_type}.json".format(
            model_name=self.MODEL_NAME,
            file_descr=file_descr,
            datetime=now.replace(":", ""),
            fc_type=fc_type
        )
        content = {
            "info": {
                "time": now,
                "fc_type": fc_type,
                "reftime_dates": list(all_dates.astype("str"))
            },
            "request": full_request
        }

        if True:  # for development purpose
            with open(path + "/" + filename, "w") as outfile:
                json.dump(content, outfile)

    @staticmethod
    def filter_reftimes(dates):
        """
        Drop dates that are no valid reftimes (realtime model initialization dates) for S2S ECMWF.
        Args:
            dates (list of np.datetime64[D]):  Dates to analyze.
        Returns:
            list: List of valid dates.
        """
        dates = np.atleast_1d(dates)
        dates_pd = pd.Series(dates)
        filtered_dates = dates[(dates_pd.dt.weekday == 0) | (dates_pd.dt.weekday == 3)]
        assert len(filtered_dates) > 0, "no dates left after filtering reftimes"
        return filtered_dates
