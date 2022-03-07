import pandas as pd


def ssw_compendium_event_dates(column='ERA-Interim'):
    # read html table from SSW compendium
    result = pd.read_html(
        "https://csl.noaa.gov/groups/csl8/sswcompendium/majorevents.html"
    )
    df_raw = result[0]

    # parse dates
    def parse_dates(x):
        if "-" in str(x):
            x = pd.to_datetime(str(x), format="%d-%b-%y")
            x = x.replace(year=x.year - 100) if x.year > datetime.now().year else x
            return x
        else:
            return x

    df = df_raw.applymap(parse_dates)

    # filter for valid dates
    erai_dates = df[column][
        df[column].apply(lambda x: isinstance(x, pd.Timestamp))
    ]

    return erai_dates
