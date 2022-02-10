from unittest import TestCase
from s2stools import events, process, plot

import matplotlib.pyplot as plt


class TestEvents(TestCase):

    def test_find_ssw(self):
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        u60_10hPa_proxy = ds.u.sel(latitude=60).mean("longitude")
        # u_fc = process.stack_fc(u60_10hPa_proxy)
        event_list = events.find_ssw(u60_10hPa_proxy, buffer_start=0, buffer_end=0, require_westwind_start=1)
        comp = events.composite_from_eventlist(event_list, ds)
        u60_10hPa_proxy_comp = comp.u.mean("longitude").sel(latitude=60)
        self.assertGreater(u60_10hPa_proxy_comp.sel(lagtime="-1D").min("i").values, 0)
        self.assertLess(u60_10hPa_proxy_comp.sel(lagtime="0D").max("i").values, 0)

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

    def test_rename_eventlist_key(self):
        eventlist = [{'fc': {'reftime': '2021-01-21', 'hc_year': -20, 'number': 0},
                      'days_since_init': 19},
                     {'fc': {'reftime': '2021-01-21', 'hc_year': -20, 'number': 3},
                      'days_since_init': 30}]
        new_eventlist = events.rename_eventlist_key(eventlist, {"days_since_init": "leadtime"})
        self.assertNotIn("leadtime", eventlist[0].keys())
        self.assertIn("days_since_init", eventlist[0].keys())
        self.assertIn("leadtime", new_eventlist[0].keys())
        self.assertNotIn("days_since_init", new_eventlist[0].keys())

    def test_format_eventlist_dayssinceinit_to_pdtimedeltastamp(self):
        eventlist = [{'fc': {'reftime': '2021-01-21', 'hc_year': -20, 'number': 0},
                      'days_since_init': 19},
                     {'fc': {'reftime': '2021-01-21', 'hc_year': -20, 'number': 3},
                      'days_since_init': 30}]
        new_eventlist = events.format_eventlist_dayssinceinit_to_pdtimedeltastamp(eventlist)
        print(new_eventlist)

    def test_EventComposite(self):
        # load data
        ds = process.open_files(path_pattern="../../data/s2s_ecmwf_uv_20*")
        # SSW index
        u60_10hPa_proxy = ds.u.sel(latitude=60).mean("longitude")  # .sel(hc_year=slice(-5, 0))
        # find events
        event_list = events.find_ssw(u60_10hPa_proxy, buffer_start=0, buffer_end=0, require_westwind_start=1)
        # create EventComposite
        ec = events.EventComposite(data=ds, event_jsons_path=event_list, descr="SSW", model="ecmwf")
        self.assertIsInstance(ec, events.EventComposite)
        # plot
        # plot.composite_overview(ec.comp.sel(latitude=60).mean("longitude"))
        # plt.show()
        # plt.close()
        # ec.plot_eventinfo()
        # plt.show()
        # plt.close()
