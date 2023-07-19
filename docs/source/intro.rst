.. _intro:

Why s2stools
============



1. File structure
-----------------

* read ``realtime`` + ``hindcasts`` and ``control`` + ``perturbed`` forecasts from different initialization dates all into a single file (:func:`s2stools.process.s2sparser`)

* make use of the dimension ``leadtime`` while obtaining ``validtime`` as a dimensionless coordinate

.. code-block:: python

    >>> ds = xr.open_mfdataset("/some/path/filename_2017*.nc", preprocess=s2stools.process.s2sparser)
    >>> ds
    <xarray.Dataset>
    Dimensions:    (leadtime: 47, longitude: 2, latitude: 1, number: 51,
                    reftime: 2, hc_year: 21)
    Coordinates:
      * leadtime   (leadtime) timedelta64[ns] 0 days 1 days ... 45 days 46 days
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
      * number     (number) int64 0 1 2 3 4 5 6 7 8 9 ... 42 43 44 45 46 47 48 49 50
      * reftime    (reftime) datetime64[ns] 2017-11-16 2017-11-20
      * hc_year    (hc_year) int64 -20 -19 -18 -17 -16 -15 -14 ... -5 -4 -3 -2 -1 0
        validtime  (reftime, leadtime, hc_year) datetime64[ns] 1997-11-16 ... 201...
    Data variables:
        u          (reftime, latitude, longitude, leadtime, hc_year, number) float32 dask.array<chunksize=(1, 1, 2, 47, 20, 1), meta=np.ndarray>


2. Deseasonalization
--------------------

* compute a robust climatology and anomalies therefrom

.. code-block:: python

    clim = s2stools.clim.climatology(
        data,
        window_size=15,       # include preceding/subsequent reftimes within window
        mean_or_std="mean",   # compute climaological mean or standard deviation
        ndays_clim_filter=7,  # apply running mean of ndays_clim_filter days
        hide_warnings=False,  # warn when not many reftimes are available for climatology
        groupby="validtime",  # apply climatology over same day-of-year (validtime) or leadtime
    )


3. Composites around events
---------------------------

* identify events and create a composite dataset that easily allows to compute e.g. the composite mean

.. code-block:: python

    >>> ssw_composite = EventComposite(data, "path/to/eventlists*.json", descr="sudden warmings", model="ecmwf")
    >>> ssw_composite
    <s2stools.events.EventComposite> of 182 events of type 'sudden warmings'.
        --> see composite dataset using my_event_composite.comp
    >>> ssw_composite.comp
    <xarray.Dataset>
    Dimensions:    (lagtime: 73, longitude: 2, latitude: 1, i: 182)
    Coordinates:
      * lagtime    (lagtime) timedelta64[ns] -36 days -35 days ... 35 days 36 days
      * longitude  (longitude) float32 -180.0 -177.5
      * latitude   (latitude) float32 60.0
        number     (i) int64 0 3 4 5 6 8 10 3 8 10 ... 39 40 42 43 44 45 46 47 48 49
        reftime    (i) datetime64[ns] 2017-11-16 2017-11-16 ... 2017-11-20
        hc_year    (i) int64 -20 -20 -20 -20 -20 -20 -20 -19 -19 ... 0 0 0 0 0 0 0 0
        validtime  (i, lagtime) datetime64[ns] NaT NaT NaT NaT ... NaT NaT NaT NaT
        leadtime   (i) timedelta64[ns] 11 days 11 days 10 days ... 19 days 16 days
      * i          (i) int64 0 1 2 3 4 5 6 7 8 ... 174 175 176 177 178 179 180 181
    Data variables:
        u          (i, latitude, longitude, lagtime) float32 dask.array<chunksize=(1, 1, 2, 73), meta=np.ndarray>

4. Furthermore
--------------

* plotting tools
* zonal wavenumber decomposition
* download S2S ECMWF data
