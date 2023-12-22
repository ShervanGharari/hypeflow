#!/bin/bash

rm -rf hypeflow_venv

python3.9 -m pip install virtualenv
python3.9 -m virtualenv hypeflow_venv
source hypeflow_venv/bin/activate

python3.9 -m pip install --upgrade pip

# assuming dependecies are installed
python3.9 -m pip install xarray
python3.9 -m pip install geopandas
python3.9 -m pip install networkx
python3.9 -m pip install jupyter
python3.9 -m pip install ipykernel
python3.9 -m pip install numpy

# add the ual env to kernel and list
python3.9 -m ipykernel install --name=hypeflow_venv # Add the new virtual environment to Jupyter
jupyter kernelspec list # list existing Jupyter virtual environments
