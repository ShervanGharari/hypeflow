#!/bin/bash

rm -rf fhimp_venv

python3.9 -m pip install virtualenv
python3.9 -m virtualenv fhimp_venv
source fhimp_venv/bin/activate

python3.9 -m pip install --upgrade pip

# assuming dependecies are installed
python3.9 -m pip install xarray
python3.9 -m pip install geopandas
python3.9 -m pip install networkx
python3.9 -m pip install jupyter
python3.9 -m pip install ipykernel

# assuming that dependecies will work
# python3.9 -m pip install -r requirements.txt

# packages to be added to requirments or release on pypi
python3.9 -m pip install git+https://github.com/kasra-keshavarz/hydrant
python3.9 -m pip install git+https://github.com/kasra-keshavarz/meshflow
python3.9 -m pip install git+https://github.com/ShervanGharari/EASYMORE.git@develop_2.0.0

# add the virtual env to kernel and list
python3.9 -m ipykernel install --name=fhimp_venv # Add the new virtual environment to Jupyter
jupyter kernelspec list # list existing Jupyter virtual environments
