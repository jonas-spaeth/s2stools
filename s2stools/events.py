def _eventlist_from_json(param):
    pass


def _dataset_ensure_leadtime_key(data):
    pass


def _eventlist_ensure_leadtime_key(event_list_json):
    pass


def _composite_from_eventlist(event_list_json, data):
    pass


class EventComposite:
    # attributes
    data = None
    comp = None
    descr = ""
    event_list_json = []

    # init
    def __init__(self, data, events, descr, model):
        """

        Parameters
        ----------
        data : xr.Dataset
            dataset with appropriate dimensions (reftime, leadtime, hc_year, number, ...)
        events : (str or list)
            if str, then define the path, e.g., 'json/s2s_events/MODEL/ssw/*' (model will automatically relpaced by self.model); if list then of format [{"fc": {"reftime": None, "hc_year": None, "number": None}, "days_since_init": None}, {...}]
        descr : str
            event description, e.g., used for plot titles
        model : str
            model name, e.g., ecmwf or ukmo

        Warnings
        --------
        Being implemented at the moment.

        """
        self.descr = descr
        self.model = model
        # events from json
        if isinstance(events, str):
            # assert that event_jsons_path describes a path
            self.event_list_json = _eventlist_from_json(events.replace("MODEL", model))
        elif isinstance(events, list):
            # assert event_jsons_path is the list of events
            self.event_list_json = events
        else:
            raise TypeError
        # assert that data has key leadtime, not days_since_init
        self.data = _dataset_ensure_leadtime_key(data)
        # assert that event_list has key leadtime, not days_since_init
        self.event_list_json = _eventlist_ensure_leadtime_key(self.event_list_json)
        # create composite from event list
        self.comp = _composite_from_eventlist(self.event_list_json, self.data)

    def __len__(self):
        return len(self.comp.i)
