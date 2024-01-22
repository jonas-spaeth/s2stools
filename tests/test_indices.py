from s2stools.indices import download_mjo, download_indices, download_enso, download_qbo


def test_download_mjo():
    download_mjo()


def test_download_indices():
    download_indices()


def test_download_qbo():
    result = download_qbo()
    print(result)
