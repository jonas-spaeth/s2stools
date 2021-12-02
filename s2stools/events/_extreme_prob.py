import xarray as xr
from tqdm import tqdm
import numpy as np


def extreme_predictors(data_comp):
    """Compute the fraction of events with nam1000 below 0, -3 and above +3.

    Args:
        data_comp (xr.Dataset): Dataset that contains variable nam1000 and dimensions "days_since_event" and "i".

    Returns:
        xr.Dataset: Dataset with dimension "days_since_event"
    """

    predictor_fractions_m2 = []
    predictor_fractions_m0 = []
    predictor_fractions_p2 = []

    for d in data_comp.days_since_event:
        data_sel = data_comp.nam1000.sel(days_since_event=d).compute()

        data_sel_extr = data_sel.where(data_sel < -3, drop=True)
        fraction = len(data_sel_extr.i) / len(data_sel.dropna("i").i)
        predictor_fractions_m2.append(fraction)

        data_sel_extr = data_sel.where(data_sel < 0, drop=True)
        fraction = len(data_sel_extr.i) / len(data_sel.dropna("i").i)
        predictor_fractions_m0.append(fraction)

        data_sel_extr = data_sel.where(data_sel > 3, drop=True)
        fraction = len(data_sel_extr.i) / len(data_sel.dropna("i").i)
        predictor_fractions_p2.append(fraction)

    return xr.Dataset(
        data_vars={
            "extremors_nam1000_m2": ("days_since_event", predictor_fractions_m2),
            "extremors_nam1000_m0": ("days_since_event", predictor_fractions_m0),
            "extremors_nam1000_p2": ("days_since_event", predictor_fractions_p2),
        },
        coords={"days_since_event": ("days_since_event", data_comp.days_since_event.values)},
    )


def prob_oneday_extreme_nam_within_period_clim(
        nam_data,
        extreme_threshold,
        bootstrap_sample_size,
        n_bootstrap_samples=1_000,
        uncertainty_percentiles=[0.025, 0.975],
        max_days=None,
):
    """Compute climatological probability of at least 1 day below threshold as function of waiting period.

    Args:
        nam_data (DataArray): data of S2S model (not stacked)
        extreme_threshold (float): threshold
        bootstrap_sample_size (int): how many forecasts to sample for one estimate
        n_bootstrap_samples (int, optional): how many times to repeat drwaing estimates. Defaults to 1_000.
        uncertainty_percentiles (list, optional): Confidence Interval. Defaults to [0.025, 0.975].
        max_days (int, optional): How many days for climatology computation, if None than =len(days_since_init). Defaults to None.

    Returns:
        DataArray: coords are {"stat": ["mean", "uncertainty_top", "uncertainty_bottom"], "days": <day_list>}
    """

    dsi_start = 10  # parameter that can be set
    if not max_days:
        max_days = len(nam_data.days_since_init)  # 35

    # iterate over period lengths

    mean = []
    uncertainty_top = []
    uncertainty_bottom = []
    for pl in tqdm(range(max_days), desc="p clim iterating period lengths"):
        # compute climatological mean and CI for p between 1 and i
        pop_mean, pop_ci = bootstrap_extr_prob(
            population=nam_data.stack(fc=("reftime", "hc_year", "number")).dropna(
                "fc", how="all"
            ),
            sample_size=bootstrap_sample_size,  # len(ssw.comp.i),
            dsi_slice=slice(dsi_start, dsi_start + pl),
            threshold=extreme_threshold,
            n_bootstrap_samples=n_bootstrap_samples,
        )
        mean.append(pop_mean)
        uncertainty_bottom.append(pop_ci[0])
        uncertainty_top.append(pop_ci[1])

    pos_lag_days = np.arange(1, max_days + 1)
    mean = np.array(mean)
    uncertainty_bottom = np.array(uncertainty_bottom)
    uncertainty_top = np.array(uncertainty_top)

    res = xr.DataArray(
        np.array([mean, uncertainty_top, uncertainty_bottom]),
        dims=["stat", "days"],
        coords={
            "stat": np.array(["mean", "uncertainty_top", "uncertainty_bottom"]),
            "days": pos_lag_days,
        },
    )
    return res


def prob_oneday_extreme_nam_within_period_after_event(nam_comp, extreme_threshold):
    """Compute probability for at least 1 day nam < extreme_threshold within period.

    Args:
        nam_comp (DataArray): comp with dims=['days_since_event', 'i']
        extreme_threshold (float): threshold

    Returns:
        DataArray: probability with variable 'days' which is the period length
    """

    period_length = len(nam_comp.sel(days_since_event=slice(1, None)).days_since_event)

    p = []
    for i in tqdm(range(period_length), desc="p clim iterating period lengths"):
        p_within_i_days_after_event = integrated_extr_prob(
            data=nam_comp, dse_slice=slice(1, 1 + i), threshold=extreme_threshold
        )
        p.append(p_within_i_days_after_event)

    pos_lag_days = np.arange(1, period_length + 1)
    p = np.array(p)
    res = xr.DataArray(p, dims=["days"], coords={"days": pos_lag_days})
    return res


def bootstrap_extr_prob(
        population, sample_size, dsi_slice, threshold, alpha=0.05, n_bootstrap_samples=1_000
):
    """Bootstrapping forecasts to compute mean and confidence interval for probability that NAM is below threshold.

    Args:
        population (DataArray): dimensions 'fc' and 'days_since_init'
        sample_size (int): size of one sample that is bootstrapped
        dsi_slice (int): at which 'days_since_init' to start to bootstrap
        threshold (int): NAM below which threshold?
        alpha (float, optional): alpha-level for confidence interval. Defaults to 0.05.
        n_bootstrap_samples (int, optional): times to repeat drawing samples from the population. Defaults to 1_000.

    Returns:
        (float, (float, float)): (mean, (CI_bottom, CI_top))
    """

    population_dsi_sliced = population.sel(days_since_init=dsi_slice)
    population_dsi_sliced_vals = population_dsi_sliced.values
    rand_fc_idx = np.random.randint(
        low=0, high=len(population.fc), size=sample_size * n_bootstrap_samples
    )
    n_dsi = len(population_dsi_sliced.days_since_init)
    population_dsi_sliced_vals_reshaped = population_dsi_sliced_vals[
                                          :, rand_fc_idx
                                          ].reshape(n_dsi, sample_size, n_bootstrap_samples)

    if threshold <= 0:
        is_extreme = (population_dsi_sliced_vals_reshaped < threshold).max(
            axis=0
        )  # is extreme on at least one day
    else:
        is_extreme = (population_dsi_sliced_vals_reshaped > threshold).max(
            axis=0
        )  # is extreme on at least one day
    n_extremes = is_extreme.sum(axis=0)  # sum over forecasts
    p_extremes = n_extremes / sample_size  # divide by sample size
    p_extremes_list = p_extremes.tolist()  # list to determine mean/ CI/...

    def empirical_cdf(array):
        N = len(array)
        x = np.sort(array)
        cdf = np.arange(N) / N
        return x, cdf

    x, cdf = empirical_cdf(p_extremes_list)
    test = np.argwhere((cdf > alpha / 2) & (cdf < 1 - alpha / 2))
    idx_low, idx_high = np.argwhere((cdf > alpha / 2) & (cdf < 1 - alpha / 2))[[0, -1]]
    ci = float(x[idx_low]), float(x[idx_high])
    mean = np.mean(p_extremes_list)
    return mean, ci


# used for p after event
def integrated_extr_prob(data, dse_slice, threshold):
    """Probability that data < threshold between dse_slice days.

    Args:
        data (DataArray): composite with coordinates i, days_since_event
        dse_slice (slice): within which time frame to look for extreme
        threshold (float): definition of extreme

    Returns:
        float: probability
    """
    # data_stacked_ssw = ssw.comp.rename(i="fc").nam1000
    data = data.rename(i="fc")

    n = []
    fc_to_exclude = []
    for dse in data.sel(days_since_event=dse_slice).days_since_event:
        if threshold <= 0:
            fc_extr = (
                data.sel(days_since_event=dse)
                    .where(data.sel(days_since_event=dse) < threshold, drop=True)
                    .fc
            )
        else:
            fc_extr = (
                data.sel(days_since_event=dse)
                    .where(data.sel(days_since_event=dse) > threshold, drop=True)
                    .fc
            )
        n_fc_extr = len(fc_extr)
        n_fc_to_exclude = len(fc_to_exclude)
        new_fc_extr = list(set(fc_extr.values) - set(fc_to_exclude))
        normalization = len(
            data.sel(
                days_since_event=dse,
                fc=np.isin(
                    data.sel(days_since_event=dse).fc, fc_to_exclude, invert=True
                ),
            )
                .dropna("fc")
                .fc
        )
        p = 0 if normalization == 0 else len(new_fc_extr) / normalization
        n.append(p)
        fc_to_exclude.extend(fc_extr.values)
        fc_to_exclude = list(set(fc_to_exclude))  # new

    return 1 - np.prod(1 - np.array(n))
