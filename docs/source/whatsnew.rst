.. _whatsnew:

What's new
=========================

Only some of the major changes are tracked here.


v0.3.5 (August 2023)
--------------------

- :func:`s2stools.utils.lat_weighted_spatial_mean` makes it quicker to average a dataset over a spatial domain, because it directly accounts for latitudinal dependece of grid box areas
- :func:`s2stools.utils.groupby_quantiles` implements an adaption of xarray's groupby_bins: while ``groupby_bins`` by default splits the data into bins of the same size, ``groupby_quantiles`` splits data into groups which have the same subset size
- :func:`s2stools.process.s2sparser` supports 6hrly data, as required for precipitation for example
- :func:`s2stools.process.s2sparser` supports data without dimension "number"
- two new colorbars, and the default kwargs for plotting on a North Atlantic map
- new features: :func:`s2stools.plot.add_map` and :func:`s2stools.plot.add_box`
- download nceps nao index to xarray: :func:`s2stools.indices.nao`
- added :func:`s2stools.compute.css` to compute correlation skill score
- fix an issue with :func:`s2stools.indices.download_mjo()`