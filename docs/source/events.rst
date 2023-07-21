.. _events:

Events (``s2stools.events``)
========================================================
Finding events in the forecasts and creating composites.

.. code-block:: python

    ssw_composite = EventComposite(data, "path/to/eventlists*.json", descr="Sudden Warmings", model="ecmwf")

Class
-------
.. autoclass:: s2stools.events.EventComposite
   :members:

   .. automethod:: __init__


Functions
---------
.. automodule:: s2stools.events
   :members:
   :exclude-members: integrated_extr_prob, prob_oneday_extreme_nam_within_period_after_event, prob_oneday_extreme_nam_within_period_clim

