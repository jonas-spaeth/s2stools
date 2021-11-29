import numpy as np


def list_to_string(l):
    if isinstance(l, str):
        return l
    if isinstance(l, range) | isinstance(l, np.ndarray):
        l = list(l)
    if isinstance(l, list):
        return "/".join([str(i) for i in l])
    else:
        assert False, "list_to_string excepts only type string or list, not {}".format(type(l))


def add_years(dt64, years):
    td_1yr = np.timedelta64(365, "D") + np.timedelta64(6, "h")
    return (dt64 + years * td_1yr).astype("datetime64[D]")


def to_timedelta64(a, assume="D"):
    """
    Assume
    Args:
        a (int or np.timedelta64): timedelta
        assume (): timedelta64 format that is assumed if a is of type int.

    Returns:
        np.timedelta64
    """
    if isinstance(a, int):
        a = np.timedelta64(a, "D")
    return a
