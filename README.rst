========
s2stools
========

.. image:: https://readthedocs.org/projects/s2stools/badge/?version=latest
    :target: https://pyzome.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


.. image:: https://img.shields.io/pypi/v/s2stools.svg
    :target: https://img.shields.io/pypi/v/s2stools


Documentation
-------------

Documentation on readthedocs: https://s2stools.readthedocs.io

Install
-------
s2stools is available on PyPI and can be installed with pip::

    pip install s2stools


Features
--------
- data handling: combine different init dates, realtime forecasts, hindcasts, control and perturbed forecasts\
  into one `xr.Dataset` by introducing dimensions `reftime`, `hc_year`, `leadtime`
- simultaneous use ``leadtime`` and ``validtime``
- climatology and deseasonalization
- find events in forecasts and create composites
- and more...

Acknowledgments
---------------
Jonas Spaeth is supported by the Transregional Collaborative Research Center “Waves to Weather” (W2W; SFB/TRR165).
