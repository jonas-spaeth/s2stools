def list_to_string(l):
    if isinstance(l, str):
        return l
    elif isinstance(l, list):
        return "/".join([str(i) for i in l])
    else:
        assert False, "list_to_string excepts only type string or list, not {}".format(type(l))
