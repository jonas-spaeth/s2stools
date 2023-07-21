import pandas as pd

from s2stools import utils
import numpy as np


class Rt:
    request_rt = {}

    def __init__(self):
        self.request_rt["stream"] = "enfo"


class RtCf(Rt):
    request = {}

    def __init__(self, rtdate=None):
        super().__init__()
        self.request = super().request_rt
        self.request["type"] = "cf"


class RtPf(Rt):
    request = {""}

    def __init__(self, rtdate=None):
        super().__init__()
        self.request = super().request_rt
        self.request["type"] = "pf"
        self.request["number"] = utils.list_to_string(self.number_all())

    def number_all(self):
        """
        Return all available ensemble members for realtime perturbed forecasts.
        Returns: String numbering the Members.

        """
        return range(1, 51)


class Hc:
    request_hc = {}

    def __init__(self, rtdate):
        self.request_hc["hdate"] = utils.list_to_string(self.hdate_all(rtdate))
        self.request_hc["stream"] = "enfh"

    def hdate_all(self, date):
        """
        All available hindcast dates.
        Args:
            date (np.datetime64): Realtime date to which the hindcast refers.

        Returns: np.ndarray(datetime64[D]) of all hindcast initialization dates

        """
        end = pd.to_datetime(date)
        if (end.month == 2) & (end.day == 29):
            start = end - pd.DateOffset(years=20) - pd.DateOffset(days=1)
        else:
            start = end - pd.DateOffset(years=20)

        hdates = pd.date_range(start, periods=20, freq=pd.DateOffset(years=1))
        return np.array(hdates, dtype="datetime64[D]")


class HcCf(Hc):
    request = {}

    def __init__(self, rtdate):
        super().__init__(rtdate=rtdate)
        self.request = super().request_hc
        self.request["type"] = "cf"
        self.request["stream"] = "enfh"
        # self.request["alias"] = "hccf"


class HcPf(Hc):
    request = {}

    def __init__(self, rtdate):
        super().__init__(rtdate=rtdate)
        self.request = super().request_hc
        self.request["type"] = "pf"
        self.request["stream"] = "enfh"
        # self.request["alias"] = "hcpf"
        self.request["number"] = self.number_all()

    def number_all(self):
        """
        Return all available ensemble members for hindcast perturbed forecasts.
        Returns: String numbering the Members.

        """
        return utils.list_to_string(range(1, 11))
