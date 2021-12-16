import pandas as pd

from ._infer_statistics import *
from ._infer_metadata import *
from ._utils import *
from ._manage import *
from ._extreme_prob import *
import matplotlib.pyplot as plt
from matplotlib import ticker
import calendar


class EventComposite:
    # attributes
    data = None
    comp = None
    descr = ""
    event_list_json = []
    plot_colors = {"primary": "darkslategray"}

    # init
    def __init__(self, data, event_jsons_path, descr, model, plot_colors=None):
        if plot_colors:
            self.plot_colors = self.plot_colors | plot_colors
        self.data = data
        self.descr = descr
        self.model = model
        # events from json
        if isinstance(event_jsons_path, str):
            # assert that event_jsons_path describes a path
            self.event_list_json = eventlist_from_json(
                event_jsons_path.replace("MODEL", model)
            )
        elif isinstance(event_jsons_path, list):
            # assert event_jsons_path is the list of events
            self.event_list_json = event_jsons_path
        else:
            raise TypeError
        # create composite from event list
        self.comp = composite_from_eventlist(self.event_list_json, data)

    def __len__(self):
        return len(self.comp.i)

    # plot functions

    def plot_composite(self, title_event_name="S2S events", save=False, save_descr=""):
        data_comp = self.comp
        n_contr_events = n_events_by_lagtime(self.comp)

        data_comp_mean = data_comp.mean("i")
        data_comp_std = data_comp.std("i")
        n_events = len(data_comp.i)

        # plot mean wind and mean nam on a relative time axis w.r.t. ssw central date
        fig, ax = plt.subplots(3, 1, figsize=(4, 5), sharex=True)
        c = self.plot_colors.get("primary")

        data_comp_mean.u60.plot(ax=ax[0], color=c)
        ax[0].fill_between(
            data_comp.lagtime.values,
            data_comp_mean.u60 + data_comp_std.u60,
            data_comp_mean.u60 - data_comp_std.u60,
            alpha=0.2,
            color=c,
        )

        ax0_2 = ax[0].twinx()
        data_comp_mean.u60anom.plot(ax=ax[0], color=c, linestyle="--", alpha=0.5)
        ax[0].set_ylim(-30, 65)
        ax[0].set_yticks(np.arange(-25, 51, 25), minor=False)
        ax[0].set_ylabel("u60, 10hPa [m/s]\n(dashed: anomaly)")
        ax[0].set_title("")
        ax0_2.plot(
            self.comp.lagtime.values,
            np.array(n_contr_events) / n_events,
            linestyle="dotted",
            c="dimgray",
            zorder=-10,
            alpha=0.6,
        )
        ax0_2.set_ylabel("events (dotted)", color="dimgray")
        ax0_2.set_yticks([0, 0.5, 1])
        ax0_2.set_yticklabels(["0%", "50%", "100%"], color="dimgray")
        ax0_2.tick_params(axis="y", colors="dimgray")

        data_comp_mean.nam200.plot(ax=ax[1], color=c)
        ax[1].fill_between(
            data_comp.lagtime.values,
            data_comp_mean.nam200 + data_comp_std.nam200,
            data_comp_mean.nam200 - data_comp_std.nam200,
            alpha=0.2,
            color=c,
        )
        ax[1].set_ylim(-2.1, 2.1)
        ax[1].set_yticks(np.arange(-2, 2.1, 0.5), minor=True)

        data_comp_mean.nam1000.plot(ax=ax[2], color=c)
        ax[2].fill_between(
            data_comp.lagtime.values,
            data_comp_mean.nam1000 + data_comp_std.nam1000 * 0.1,
            data_comp_mean.nam1000 - data_comp_std.nam1000 * 0.1,
            alpha=0.2,
            color=c,
        )
        ax[2].set_ylim(-2.1, 2.1)
        ax[2].set_yticks(np.arange(-2, 2.1, 0.5), minor=True)

        ax[0].set_xlabel("")
        ax[0].set_title("")
        ax[1].set_xlabel("")
        ax[1].set_title("")
        ax[1].set_ylabel("NAM200")
        ax[2].set_xlabel("lag [days]")
        ax[2].set_title("")
        ax[2].set_ylabel("NAM1000")

        for x in np.append(ax, ax0_2):
            x.spines["top"].set_visible(False)
            x.spines["right"].set_visible(False)
            x.spines["bottom"].set_visible(False)
            if x != ax0_2:
                x.grid(alpha=0.45)
                x.grid(which="minor", alpha=0.15)
                x.axhline(0, color="black", alpha=0.3, linewidth=1)
                x.axvline(0, color="black", alpha=0.3, linewidth=1)
                x.xaxis.set_major_locator(ticker.MultipleLocator(7))
                x.set_xlim(
                    data_comp.lagtime[0], data_comp.lagtime[-1]
                )

        fig.suptitle("Composite over {} {}".format(n_events, title_event_name))
        if save:
            fig.savefig(
                "ssw_composite4/composite_{}_{}.pdf".format(self.descr, save_descr),
                bbox_inches="tight",
            )
        return fig, np.append(ax, ax0_2)

    def plot_percentiles(self, save=False, save_kw=""):
        statistics = data_statistics(self.comp)
        quantiles = data_percentiles(self.comp)

        data_comp_mean, data_comp_std, data_comp_min, data_comp_max = statistics

        fig, ax = plt.subplots(figsize=(7, 4))

        # mean, min, max
        c_mean = "black"
        data_comp_mean.nam1000.plot(ax=ax, color="whitesmoke", lw=2, label="Mean")
        data_comp_min.nam1000.plot(ax=ax, color="black", lw=0.5, label="Min")
        data_comp_max.nam1000.plot(ax=ax, color="black", lw=0.5, label="Max")

        # quantiles
        ax.fill_between(
            self.comp.lagtime,
            data_comp_min.nam1000,
            quantiles["2"].nam1000,
            alpha=0.2,
            color="lightgrey",
            label="0-2 Percentile",
        )
        ax.fill_between(
            self.comp.lagtime,
            quantiles["10"].nam1000,
            quantiles["90"].nam1000,
            alpha=0.9,
            color="lightgrey",
            label="10-90 Percentile",
        )
        ax.fill_between(
            self.comp.lagtime,
            quantiles["30"].nam1000,
            quantiles["70"].nam1000,
            alpha=0.9,
            color="grey",
            label="30-70 Percentile",
        )
        ax.fill_between(
            self.comp.lagtime,
            quantiles["98"].nam1000,
            data_comp_max.nam1000,
            alpha=0.2,
            color="lightgrey",
            label="98-100 Percentile",
        )

        # auxilary plot lines
        ax.axhline(0, color="darkcyan", alpha=1, linewidth=1)
        ax.axvline(0, color="darkcyan", alpha=1, linewidth=1)

        # plot appearance
        ylim = max(np.abs(ax.get_ylim()))
        ax.set_ylim(-ylim, ylim)
        ax.grid(alpha=0.3)
        ax.set_xticks(np.arange(-21, 22, 7))
        ax.xaxis.set_major_locator(ticker.MultipleLocator(7))

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_visible(False)

        # legend
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), frameon=False)

        # finalize
        if save:
            raise NotImplementedError
            # fig.savefig(
            #     DIR_PLOTS
            #     + script_name
            #     + "s2s_downcoup_{}{}{}_percentiles.pdf".format(setup, winter, save_kw)
            # )
        return fig, ax

    def plot_eventinfo(self, save=True, save_kw=""):

        event_dates = event_dates_from_ds(
            self.comp, model=self.model
        ).rename("all")
        event_dates_rt = event_dates_from_ds(
            self.comp.where(self.comp.hc_year == 0, drop=True), model=self.model
        ).rename("realtime")
        event_dates_hc = event_dates_from_ds(
            self.comp.where(self.comp.hc_year != 0, drop=True), model=self.model
        ).rename("hindcast")

        event_leadtimes = event_leadtimes_from_ds(self.comp)
        event_leadtimes_rt = event_leadtimes_from_ds(
            self.comp.where(self.comp.hc_year == 0, drop=True)
        )
        event_leadtimes_hc = event_leadtimes_from_ds(
            self.comp.where(self.comp.hc_year != 0, drop=True)
        )
        fc_day_dates_permonth_all = fc_day_dates_permonth(self.data).rename(
            "all"
        )
        fc_day_dates_permonth_rt = fc_day_dates_permonth(
            self.data.where(self.data.hc_year == 0, drop=True)
        ).rename("realtime")
        # fc_day_dates_permonth_hc = local_utils.fc_day_dates_permonth(
        #     self.data.where(self.data.hc_year != 0, drop=True)
        # ).rename("hindcast")

        # plot
        fig, axs = plt.subplots(
            2,
            2,
            figsize=(8, 5),  # (11.5, 3),
            gridspec_kw={"width_ratios": [3, 1]},
        )
        color = "teal"

        ax = axs[0, 0]  # *** BY WINTER SEASON ***

        count_seas_all = (
            event_dates.groupby(event_dates.map(date_to_winter_season))
                .count()
                .rename("all")
        )
        count_seas_rt = (
            event_dates_rt.groupby(
                event_dates_rt.map(date_to_winter_season)
            )
                .count()
                .rename("realtime")
        )
        count_seas_hc = (
            event_dates_hc.groupby(
                event_dates_hc.map(date_to_winter_season)
            )
                .count()
                .rename("hindcast")
        )

        count_seas_df = pd.concat(
            [count_seas_all, count_seas_rt, count_seas_hc], axis=1
        )
        hatch_kws = dict(color="yellowgreen", edgecolor=color, hatch="//")
        plane_kws = dict(color=color, edgecolor=color)
        # plot all forecasts hatched, then the hindcasts in solid, so that only the rt appear hatched
        count_seas_df["all"].rename("realtime").plot.bar(ax=ax, **hatch_kws)
        count_seas_df["hindcast"].plot.bar(ax=ax, **plane_kws)
        ax.set_xlabel("by winter")
        ax.set_ylabel("absolute frequency")
        # for legend
        xlim = ax.get_xlim()
        ax.bar([0], [0], color="k", edgecolor="k", label="all", linewidth=0)
        ax.set_xlim(xlim)

        ax = axs[0, 1]  # *** BY MONTH ***

        count_month_df = (
            pd.concat([pd.to_datetime(event_dates), pd.to_datetime(event_dates_rt)], axis=1)
                .applymap(lambda t: t.month)
                .apply(pd.Series.value_counts)
        )
        fc_day_dates_permonth_df = pd.concat(
            [fc_day_dates_permonth_all, fc_day_dates_permonth_rt], axis=1
        )
        ax.axis("off")

        n_fc = (
            count_month_df["all"]
                .reindex(list(range(7, 13)) + list(range(1, 7)))
                .dropna(how="all")
                .values
        )
        lbls = ["{:.0f}".format(n) for n in n_fc]  ## fc:

        ax = axs[1, 1]  # *** BY MONTH (NORMALIZED) ***

        count_month_df_hc_normalized = count_month_df / fc_day_dates_permonth_df
        count_month_df_hc_normalized = (
            count_month_df_hc_normalized[["all"]]
                .reindex(list(range(7, 13)) + list(range(1, 7)))
                .dropna(how="all")
        )
        p = count_month_df_hc_normalized.plot.bar(
            ax=ax, legend=False, width=0.6, color="k", edgecolor="k"
        )
        ax.set_xlabel("by month")
        ax.set_ylabel("relative frequency\nper day forecast")
        ax.set_title("labels: abs. freq.", fontdict={"size": 10})
        annotate_bars(ax, lbls)

        # by month abs and norm
        def labels_nums_to_months(labels):
            month_dict = {
                index: month for index, month in enumerate(calendar.month_abbr)
            }
            labels_num = [t.get_text() for t in labels]
            labels_str = list(map(lambda x: month_dict[int(x)], labels_num))
            return labels_str

        for ax in [axs[1, 1]]:  # [axs[0, 1], axs[1, 1]]:
            for patch in ax.patches:
                patch.set_hatch("//")
            labels_str = labels_nums_to_months(ax.get_xticklabels())
            ax.set_xticklabels(labels_str)

        ax = axs[1, 0]  # *** BY LEADTIME ***

        count_leadtime = event_leadtimes.groupby(event_leadtimes).count()
        # count_leadtime_rt = event_leadtimes_rt.groupby(event_leadtimes_rt).count()
        count_leadtime_hc = event_leadtimes_hc.groupby(event_leadtimes_hc).count()

        # count_leadtime.plot.bar(ax=ax, color=color, edgecolor=color)
        count_leadtime.plot.bar(ax=ax, color="yellowgreen", edgecolor=color, hatch="//")
        count_leadtime_hc.plot.bar(ax=ax, color=color)
        ax.set_xlabel("by leadtime [days]")
        ax.set_ylabel("absolute frequency")
        ax.xaxis.set_major_locator(ticker.MultipleLocator(5))

        for x in axs.flatten():
            x.spines["top"].set_visible(False)
            x.spines["left"].set_visible(False)
            x.spines["right"].set_visible(False)
            x.tick_params(length=0)
            x.grid(axis="y", alpha=0.5)
        fig.tight_layout()
        fig.subplots_adjust(top=0.92, wspace=0.3)

        handles, labels = axs[0, 0].get_legend_handles_labels()
        labels = [
            "realtime ({})".format(len(event_dates_rt)),
            "hindcast ({})".format(len(event_dates_hc)),
            "all ({})".format(len(event_dates)),
        ]

        axs[1, 1].legend(
            handles[::-1],
            labels[::-1],
            loc="lower center",
            bbox_to_anchor=(0.5, 1.9),
            frameon=False,
            ncol=1,
        )

        if save:
            pass
            # fig.savefig(
            #     DIR_PLOTS
            #     + script_name
            #     + "s2s_downcoup_{}{}_eventinfo.pdf".format(setup, save_kw)
            # )
            # return
            #        event_dates,
            #        event_dates_rt,
            #        count_month_df,
            #        fc_day_dates_permonth_df,
        return fig, axs

    def extreme_probability_plot(
            self,
            save=True,
            save_kw="",
            title_event_name="S2S events",
    ):

        extreme_probability = extreme_predictors(self.comp)
        dse = self.comp.lagtime.values

        def compute_base_probabilities(thresholds):
            res = []
            stacked_days = self.data.stack(
                days=("reftime", "hc_year", "number", "leadtime")
            )
            n_tot = len(stacked_days.dropna("days").days)
            for thr in thresholds:
                if thr <= 0:
                    n = len(
                        stacked_days.where(stacked_days.nam1000 < thr, drop=True).days
                    )
                else:
                    n = len(
                        stacked_days.where(stacked_days.nam1000 > thr, drop=True).days
                    )
                res.append(n / n_tot)
            return res

        base_probabilities = compute_base_probabilities(thresholds=[0, -3, 3])

        fig, ax = plt.subplots(3, 1, figsize=(5, 5), sharex=True)

        c = self.plot_colors.get("primary")  # "darkslategray"

        da_m0 = extreme_probability.extremors_nam1000_m0
        da_m2 = extreme_probability.extremors_nam1000_m2
        da_p2 = extreme_probability.extremors_nam1000_p2

        # ****************
        x = ax[0]

        plt1 = da_m0.plot(ax=x, color=c)
        if base_probabilities:
            x.axhline(base_probabilities[0], color=c, alpha=0.5, linestyle="--")
            x.fill_between(dse, da_m0, base_probabilities[0], color=c, alpha=0.2)
        x.set_ylabel("NAM1000 < 0")
        x.set_ylim(base_probabilities[0] * 0.7, base_probabilities[0] * 1.3)
        x.text(
            dse[-1] + 1,
            base_probabilities[0] * 1.01,
            "{:.3f}".format(base_probabilities[0]),
            color=c,
        )
        x.spines["bottom"].set_visible(False)

        # ****************
        x = ax[1]

        plt2 = da_m2.plot(ax=x, color=c)
        if base_probabilities:
            x.axhline(base_probabilities[1], color=c, alpha=0.5, linestyle="--")
            x.fill_between(dse, da_m2, base_probabilities[1], color=c, alpha=0.2)
        x.set_ylabel("NAM1000 < −3")
        x.set_ylim(0, 0.016)
        x.text(
            dse[-1] + 1,
            base_probabilities[1] * 1.01,
            "{:.3f}".format(base_probabilities[1]),
            color=c,
        )
        # x.annotate("", (0, 0.012), (28, 0.012), arrowprops={"arrowstyle": "<->"})
        # integrated_prob = 1 - float(
        #     (1 - da_m2.sel(lagtime=slice(1, 28))).prod("lagtime")
        # )
        # integrated_prob_clim = 1 - float((1 - base_probabilities[1]) ** 28)
        # x.text(
        #     14,
        #     0.013,
        #     "{:.1%} ↗︎ {:.1%}".format(integrated_prob_clim, integrated_prob),
        #     ha="center",
        # )

        # ****************
        x = ax[2]

        plt2 = da_p2.plot(ax=x, color=c)
        if base_probabilities:
            x.axhline(base_probabilities[2], color=c, alpha=0.5, linestyle="--")
            x.fill_between(dse, da_p2, base_probabilities[2], color=c, alpha=0.2)
        x.set_ylabel("NAM1000 > +3")
        x.set_ylim(0, 0.016)
        x.text(
            dse[-1] + 1,
            base_probabilities[2] * 1.01,
            "{:.3f}".format(base_probabilities[2]),
            color=c,
        )

        for x in ax:
            x.spines["top"].set_visible(False)
            x.spines["right"].set_visible(False)
            x.grid(alpha=0.4)
            x.set_xlabel("")
            x.xaxis.set_major_locator(ticker.MultipleLocator(7))
            x.tick_params(axis="both", which="both", length=0)
            x.axvline(0, color="black", alpha=0.3, lw=1)
        ax[2].set_xlabel("lagtime")

        fig.tight_layout()
        fig.suptitle("fraction of {} with extreme NAM1000".format(title_event_name))
        fig.subplots_adjust(top=0.94, wspace=0.4)

        if save:
            pass
            # fig.savefig(
            #     DIR_PLOTS
            #     + script_name
            #     + "s2s_downcoup_{}_extremes.pdf".format(save_kw),
            #     bbox_inches="tight",
            # )

        return fig, ax

    # in development
    def plot_prob_oneday_extreme_within_period(
            self,
            extreme_threshold,
            fig_ax=None,
            p_clim_kws={},
            p_after_event_kws={},
            lineplot_kws={},
            hide_print=True,
    ):
        """Create a plot with period length on x-Axis and Porbability of at least one day
        with NAM < threshold on y-axis. Include Climatology of Model and the Probability
        after an event.

        Args:
            extreme_threshold (float): P( nam < threshold )
            fig_ax ((plt.figure, plt.axis), optional): Existing fig and ax can be provided as tuple. Defaults to None.
            p_clim_kws (dict, optional): Keywords passed to 'prob_oneday_extreme_nam_within_period_clim'. Defaults to {}.
            p_after_event_kws (dict, optional): Keywords passed to 'prob_oneday_extreme_nam_within_period_after_event'. Defaults to {}.
            lineplot_kws (dict, optional): Keywords passed to DataArray.plot(). Defaults to {}.
            hide_print (bool, optional): Hide status prints. Defaults to True.

        Returns:
            (plt.figure, plt.axis): fig, ax of plot
        """

        ### COMPUTATION

        # compute own climatology: mean and uncertainty
        if not hide_print:
            print("Compute Climatological P...")
        p_clim_kws_updated = (
                dict(
                    nam_data=self.data.nam1000,
                    extreme_threshold=extreme_threshold,
                    bootstrap_sample_size=len(self.comp.i),
                    n_bootstrap_samples=1_000,
                )
                | p_clim_kws
        )
        p_clim = prob_oneday_extreme_nam_within_period_clim(
            **p_clim_kws_updated
        )
        # compute appearance in comp for positive lags
        if not hide_print:
            print("Compute P After Event...")
        p_after_event_kws = (
                dict(nam_comp=self.comp.nam1000, extreme_threshold=extreme_threshold)
                | p_after_event_kws
        )
        p_after_event = prob_oneday_extreme_nam_within_period_after_event(
            **p_after_event_kws
        )

        ### PLOTTING
        if not hide_print:
            print("Plot...")
        if fig_ax is None:
            fig, ax = plt.subplots(1, 1, figsize=(5.5, 4))
        else:
            fig, ax = fig_ax

        c_prim = self.plot_colors.get("primary")
        plot_kws_clim = {"c": c_prim, "ls": "solid", "lw": 2, "ax": ax} | lineplot_kws
        plot_kws_after_event = {
                                   "c": c_prim,
                                   "ls": "--",
                                   "lw": 2,
                                   "ax": ax,
                               } | lineplot_kws

        # plot p climatology
        p_clim.sel(stat="mean").plot(
            **plot_kws_clim,
            label="S2S {}: climatology (incl. 95%-CI)".format(self.model),
        )
        x = p_clim.days.values
        y1 = p_clim.sel(stat="uncertainty_bottom").values
        y2 = p_clim.sel(stat="uncertainty_top").values
        ax.fill_between(x, y1, y2, color=plot_kws_clim.get("c"), alpha=0.15, zorder=-3)

        # plot p after event
        key = "after_event"
        p_after_event.plot(
            **plot_kws_after_event,
            label="S2S {}: following {}".format(self.model, self.descr),
        )

        # plot appearance
        ax.minorticks_on()
        ax.set_xlim(1)
        ax.set_ylim(0)
        ax.grid(alpha=0.5)
        ax.set_xlabel("t [d]")
        ax.set_ylabel("probability")
        ax.set_title(
            "$P( \geq 1 $ day $nam1000<{}$ within days 1...t $)$".format(
                extreme_threshold
            )
        )
        ax.xaxis.set_major_locator(plt.MultipleLocator(5))
        ax.tick_params(axis="both", direction="in", which="both")
        ax2 = ax.secondary_yaxis("right")
        ax2.minorticks_on()
        ax2.tick_params(axis="y", direction="in", which="both")
        ax2.set_yticklabels(ax2.get_yticklabels())

        return fig, ax
