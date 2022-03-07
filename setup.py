from setuptools import setup

VERSION = '0.0.14'
DESCRIPTION = 'python tools for handling s2s forecast data'
LONG_DESCRIPTION = 'python tools for handling s2s forecast data.' \
                   'Including submodules: ' \
                   '<download> for retreiving s2s forecasts, ' \
                   '<clim> for deseasonalization, ' \
                   '<events> for creating event composites, ' \
                   '<plot> for matplotlib utilities, ' \
                   '<process> for handling data structures, ' \
                   '<utils> for further utilities and <tests>'

setup(
    name='s2stools',
    version=VERSION,
    author='Jonas Spaeth',
    author_email='jonas.spaeth@physik.uni-muenchen.de',
    packages=['s2stools', 's2stools.clim', "s2stools.download", "s2stools.download.ecmwf", "s2stools.events",
              "s2stools.plot", "s2stools.process", "s2stools.tests", "s2stools.utils"],
    scripts=[],  # ['bin/script1','bin/script2'],
    # url='http://pypi.python.org/pypi/PackageName/',
    license='LICENSE.txt',
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=[
        "numpy",
        "xarray",
        "ecmwf-api-client"
    ],
)
