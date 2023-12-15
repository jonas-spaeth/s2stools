.. _whatsnew:

What's new
=========================

Only some of the major changes are tracked here.


v0.3.6 (unpublished)
--------------------

- :func:`s2stools.plot.cmap_spread` accepts to new arguments ``less_dark_blue`` and ``less_dark_red`` to control saturation limits
- new function :func:`s2stools.plot.legend_colored_labels_no_lines` behaves like ``ax.legend``, but instead of drawing handles and labels next to each other, only labels are created, but in the color of the respective handle
- new functions :func:`s2stools.plot.set_xticks_with_colored_labels` and :func:`s2stools.plot.set_yticks_with_colored_labels` makes colored xticklabels
- new function :func:`s2stools.plot.add_map_inset` adds a small map to an existing axis, which can be used to indicate the geographic domain of a particular analysis
- experimental: new function :func:`s2stools.compute.register_sample_variance_aggregation_for_flox` registers a function ``"sample_variance"`` to flox, which behaves like variance, but with ``ddof=1``
- :func:`s2stools.clim.climatology` now accepts hindcast-only datasets
- :func:`s2stools.clim.climatology` now raises a warning if used with flox (we should support flox in the future, do you want to implement it?)


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