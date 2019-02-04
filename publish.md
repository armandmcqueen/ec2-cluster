https://packaging.python.org/tutorials/packaging-projects/

pip install setuptools wheel
pip install twine

python3 setup.py sdist bdist_wheel

python3 -m twine upload dist/*