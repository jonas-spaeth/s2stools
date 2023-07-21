.. _download:

Download S2S ECMWF Data (``s2stools.download``)
========================================================
Enables downloads of control, perturbed, realtime, hindcast forecasts with a single setting.

-----

Here is how the code can be used, e.g., in a jupyter notebook:

In ``notebook.ipynb``:

.. code-block:: python

    from s2stools.download.ecmwf import S2SDownloaderECMWF

    dl = S2SDownloaderECMWF()
    # dates = np.arange("2018-02-22", "2018-02-23", dtype="datetime64[D]")
    dates = np.arange("2020-09-01", "2020-10-01", dtype="datetime64[D]")
    # dates = np.arange("2020-11-16", "2021-02-23", dtype="datetime64[D]")

    dl.retreive(
        param=["u"],
        file_descr="u60_10hPa",
        reftime=dates,
        #    plevs=[1000, 850, 500, 300, 200, 100, 50, 10],
        plevs=[10],
        step="all",
        path="/path/to/folder",
        grid="2.5/2.5",
        area="60/180/60/-180",  # N E S W
        rt_cf_kwargs=dict(levtype="pl", skip=False),
        rt_pf_kwargs=dict(levtype="pl", skip=False),
        hc_cf_kwargs=dict(levtype="pl", skip=False),
        hc_pf_kwargs=dict(levtype="pl", skip=False),
        write_info_file=False,  # create a json file saving the full request
    )

-------

Using a slurm cluster, realtime-control, realtime-perturbed, hindcast-control and \
hindcast-perturbed can be downloaded separately.

In ``retreive.py``:

.. code-block:: python

    import s2stools
    from s2stools.download.ecmwf import S2SDownloaderECMWF
    import sys

    sys.path.append('..')

    """
    input == 0:  rt_cf
    input == 1:  rt_pf
    input == 2:  hc_cf
    input == 3:  hc_pf
    """


    skip_rt_cf, skip_rt_pf, skip_hc_cf, skip_hc_pf = (True, True, True, True)

    print("sys.argv[1]: ", sys.argv[1])


    if sys.argv[1] == "0":
        skip_rt_cf = False
    elif sys.argv[1] == "1":
        skip_rt_pf = False
    elif sys.argv[1] == "2":
        skip_hc_cf = False
    elif sys.argv[1] == "3":
    elif sys.argv[1] == "3":
        skip_hc_pf = False

    dl = S2SDownloaderECMWF()

    dates = s2stools.download.utils.mid_nov_to_end_feb_inits([2015, 2016])

    dl.retreive(
        param=["gh"],
        file_descr="z1000",
        reftime=dates,
        plevs=[1000],
        step="all",
        path="/path/to/folder",
        grid="2.5/2.5",
        area="90/-180/0/180",
        rt_cf_kwargs=dict(levtype="pl", skip=skip_rt_cf),
        rt_pf_kwargs=dict(levtype="pl", skip=skip_rt_pf),
        hc_cf_kwargs=dict(levtype="pl", skip=skip_hc_cf),
        hc_pf_kwargs=dict(levtype="pl", skip=skip_hc_pf),
        write_info_file=True
    )


In ``submit_retreival.sh``:

.. code-block:: bash

    #!/bin/bash -l
    #SBATCH --time=20:00:00
    #ignoreSBATCH --partition=met-cl
    #SBATCH --mem=4G
    #SBATCH --array=0-3

    source /path/to/venv/bin/activate

    case "$SLURM_ARRAY_TASK_ID" in
    "0")
        python3 retreive.py 0
        ;;
    "1")
        python3 retreive.py 1
        ;;
    "2")
        python3 retreive.py 2
        ;;
    "3")
        python3 retreive.py 3
        ;;
    *)
        echo "Invalid Array Task ID"
        ;;
    esac

