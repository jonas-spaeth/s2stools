import matplotlib as mpl
from cycler import cycler


def beach_towel():
    c1 = "#fe4a49"
    c2 = "#2ab7ca"
    c3 = "#fed766"

    mpl.rcParams.update(mpl.rcParamsDefault)
    mpl.rcParams["lines.linewidth"] = 3
    mpl.rcParams["axes.prop_cycle"] = cycler(color=[c1, c2, c3, "yellowgreen", "navy", "orchid", "tan", "teal"])
    mpl.rcParams["font.size"] = 12
    mpl.rcParams["legend.fontsize"] = "small"
    mpl.rcParams["font.weight"] = "light"
    mpl.rcParams["xtick.minor.visible"] = True
    mpl.rcParams["ytick.minor.visible"] = True
    mpl.rcParams["axes.facecolor"] = "white"
    mpl.rcParams["axes.xmargin"] = 0
    mpl.rcParams["axes.ymargin"] = 0.05
    mpl.rcParams["figure.figsize"] = (4, 3)


def reset():
    mpl.rcParams.update(mpl.rcParamsDefault)
