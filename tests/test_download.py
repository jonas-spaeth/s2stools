from s2stools.download.utils import mid_nov_to_end_feb_inits
from s2stools.download.ecmwf import S2SDownloaderECMWF
from pathlib import Path
import os


def _get_skip_download_ecmwf():
    skip_download_ecmwf_envvar = os.environ.get("SKIP_DOWNLOAD_TEST", "TRUE")
    skip_download_ecmwf = True if skip_download_ecmwf_envvar == "TRUE" else False
    return skip_download_ecmwf


def test_download_ecmwf():
    try:
        import ecmwfapi
    except:
        print(
            "test requires ecmwfapi. Consider: pip install ecmwfapi. You will also need a token, see here: "
            "https://api.ecmwf.int/v1/key/"
        )
        return None

    print("SKIP = ", _get_skip_download_ecmwf())

    if _get_skip_download_ecmwf():
        # switch this test on by setting the environment variable SKIP_DOWNLOAD_TEST=FALSE
        print("download ecmwf test is skipped")
        return None

    """
    input == 0:  rt_cf
    input == 1:  rt_pf
    input == 2:  hc_cf
    input == 3:  hc_pf
    """

    skip_rt_cf, skip_rt_pf, skip_hc_cf, skip_hc_pf = (False, True, True, True)

    dl = S2SDownloaderECMWF()

    dates = mid_nov_to_end_feb_inits([2015, 2016])

    dates = dates[:2]

    dl.retreive(
        param=["gh"],
        file_descr="z1000download",
        reftime=dates,
        plevs=[1000],
        step="all",
        path="data/",
        grid="5/5",
        area="60/170/50/180",
        rt_cf_kwargs=dict(levtype="pl", skip=skip_rt_cf),
        rt_pf_kwargs=dict(levtype="pl", skip=skip_rt_pf),
        hc_cf_kwargs=dict(levtype="pl", skip=skip_hc_cf),
        hc_pf_kwargs=dict(levtype="pl", skip=skip_hc_pf),
        write_info_file=True,
    )
    file_paths = [
        "data/s2s_ecmwf_z1000download_2015-11-16_rt_cf.nc",
        "data/s2s_ecmwf_z1000download_2015-11-19_rt_cf.nc",
    ]

    for file_path in file_paths:
        file = Path(file_path)
        # check if file exists, if yes, delete
        assert file.exists()
        file.unlink()

    # delete write request info file
    from glob import glob

    json_files = glob("data/request*.json")
    for f in json_files:
        Path(f).unlink()
