from setuptools import setup

VERSION = '0.0.5'
DESCRIPTION = 'My first Python package'
LONG_DESCRIPTION = 'My first Python package with a slightly longer description'  # open('README.txt').read()

setup(
    name='s2stools',
    version=VERSION,
    author='Jonas Spaeth',
    author_email='aac@example.com',
    packages=['s2stools', 's2stools.clim', "s2stools.download", "s2stools.download.ecmwf", "s2stools.events", "s2stools.plot", "s2stools.process", "s2stools.tests", "s2stools.utils"],
    scripts=[],  # ['bin/script1','bin/script2'],
    #url='http://pypi.python.org/pypi/PackageName/',
    license='LICENSE.txt',
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=[
        "numpy",
        "xarray",
        "ecmwf-api-client"
    ],
)
