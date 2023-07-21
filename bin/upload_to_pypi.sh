cd ..
rm -d -r build
rm -d -r dist
rm -d -r s2stools.egg-info
python3 setup.py sdist bdist_wheel
twine upload --repository pypi dist/*
