#!/bin/bash
cd ..
rm -d -r build
rm -d -r dist
rm -d -r s2stools.egg-info
python3 setup.py sdist bdist_wheel
twine upload --repository pypi dist/*

# Create a Git tag
cd ..
version=$(<VERSION)
# Read version from the VERSION file
git tag -a "v$version" -m "Version $version"
# Push the tag to the remote repository
git push origin "v$version"
