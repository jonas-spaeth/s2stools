# s2stools

<details><summary>Why</summary>
E.g., ECWMF produces hindcasts on the fly. s2stools can make the data handling easier by introducing new dimensions (reftime, hc_year, leadtime). All functionalities are based on xarray `DataArrays` and `Datasets`.
</details>

<details><summary>Installation</summary>

```pip install s2stools```

This should install version 0.0.18, because the most current version (0.0.21) sometimes fails due to a circular import. This will be fixed in the near future.
</details>

# Specific Features

## Read Files and reorganize Dimensions

s2stools reads realtime forecasts + hindcasts and control + perturbed forecasts at the same time and merges the data into one dataset.

Raw ECMWF S2S forecasts come with dimension *time*. Opening files with `s2stools` changes the dimensions to *reftime* (`np.datetime64`), *hc_year* (`int`, from 0 to -20) and *leadtime* (`np.timedelta64`).

<details><summary>Advantages</summary>
- quicker access to individual hindcast years, easier to compare different hindcast years
- no discontinous time dimension
- easier to compare the leadtime evolution of different forecasts
</details>

## Adding the Validtime

`s2stools.process.add_validtime()` can be used to add a coordinate `validtime` to a dataset.


## Computing Climatology and Anomalies

`s2stools.clim.deseasonalize(standardize=True)` can be used to compute anomalies following Spaeth and Birner Appendix 1 (under revision, https://doi.org/10.5194/wcd-2021-77).

## Event Composites

`s2stools.events.EventComposite(dataset, eventlist, descr="SSWs", model="ECWMF")` can be used to create a time lagged composite around a provided list of events. Coordinate *leadtime* is changed to *lagtime* (`np.timedelta64`).

## Plotting

`s2stools.plot.xaxis_unit_days(multiple=7, ax=ax)` can be used to correctly label the timedelta x-axis (`np.timedelta64`) in units of days (not nanoseconds, what is automatically displayed).

# More information?[](url)

I'll try to come up with an actual documentation, for now see these [example notebooks](https://gitlab.lrz.de/ru35hub/stos/-/tree/main/s2stools/examples).

# Contribute

yes, please



** contact: **

jonas.spaeth@physik.uni-muenchen.de
