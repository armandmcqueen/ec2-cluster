https://packaging.python.org/tutorials/packaging-projects/

pip install setuptools wheel
pip install twine

rm -rf build
rm -rf dist
rm -rf ec2_cluster.egg-info

python3 setup.py sdist bdist_wheel
python3 -m twine upload dist/*