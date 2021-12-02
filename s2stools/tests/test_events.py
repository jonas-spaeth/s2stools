from unittest import TestCase
from s2stools import events, process


# import matplotlib.pyplot as plt


class TestEvents(TestCase):

    def test_find_ssw(self):
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        u60_10hPa_proxy = ds.u.sel(latitude=60).mean("longitude")
        u_fc = process.stack_fc(u60_10hPa_proxy)
        event_list = events.find_ssw(u60_10hPa_proxy, buffer_start=0, buffer_end=0, require_westwind_start=1)
        comp = events.composite_from_eventlist(event_list, ds)
        u60_10hPa_proxy_comp = comp.u.mean("longitude").sel(latitude=60)
        self.assertGreater(u60_10hPa_proxy_comp.sel(lagtime="-1D").min("i").values, 0)
        self.assertLess(u60_10hPa_proxy_comp.sel(lagtime="0D").min("i").values, 0)

    def test_eventdict_to_json(self):
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        u60_10hPa_proxy = ds.u.sel(latitude=60).mean("longitude")
        u_fc = process.stack_fc(u60_10hPa_proxy)
        eventdict = events.find_ssw(u60_10hPa_proxy, buffer_start=0, buffer_end=0, require_westwind_start=1)
        path = "../data/events/ssw/pseudossw.json"
        events.eventdict_to_json(eventdict, path, split_reftimes=True)

    def test_composite_from_json(self):
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        comp = events.composite_from_json("../../data/events/ssw/*.json", ds)
        self.assertIn("i", list(comp.dims))
