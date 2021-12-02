def stack_fc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year", "number")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year", "number"))


def stack_ensfc(d, reset_index=True):
    if reset_index:
        return d.stack(fc=("reftime", "hc_year")).reset_index("fc")
    else:
        return d.stack(fc=("reftime", "hc_year"))
