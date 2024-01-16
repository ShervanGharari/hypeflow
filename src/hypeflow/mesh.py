"""
This package automates MESH model setup through a flexible workflow that can
be set up using a JSON configuration file, a Command Line Interface (CLI) or
directly inside a Python script/environment.

Much of the work has been adopted from workflows developed by Dr. Ala Bahrami
and Cooper Albano at the University of Saskatchewan applying MESH modelling
framework to the North American domain.
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
import pint

from typing import (
    Dict,
    Sequence,
    Union,
)

import re
import json
import glob
import os
import sys

# from ._default_dicts import (
#     ddb_global_attrs_default,
#     ddb_local_attrs_default,
#     forcing_local_attrs_default,
#     forcing_global_attrs_default,
#     default_attrs,
# )
# from meshflow import utility


class MESHWorkflow(object):
    """
    Main Workflow class of HYPE


    Attributes
    ----------


    Parameters
    ----------


    """
    # main constructor
    # def __init__(self) -> None:
    
    
    def MESH_Dranage_database (self,
                              ntopo,
                              ntopo_mapping,
                              ntopo_rename_dims,
                              GRU,
                              GRU_vars_mapping,
                              GRU_rename_dims):
        
        GRU_mapping = {'combination':'combinations'}
                           GRU_Frac_mapping = {'subbasin': 'COMID'}
        
        # ntopo_mapping = {'subbasin': 'COMID',\
        #                                     'ChnlSlope': 'slope',\
        #                                     'ChnlLength': 'length',\
        #                                     'Rank': 'Rank',\
        #                                     'Next': 'Next',\
        #                                     'GridArea': 'unitarea',\
        #                                     'lat': 'latitude',\
        #                                     'lon': 'longitude'},
        
        
        # read ntopo check the rank frist and assign the id as index
        flipped_ntopo_mapping = {v: k for k, v in ntopo_mapping.items()}

        # Renaming columns based on flipped values
        ntopo = ntopo.rename_vars(columns=flipped_ntopo_mapping)

        # sort based on rank to make sure rank is from 1 to n
        from easymore import Utility
        ntopo = Utility.sorted_subset(ntopo, ntopo['subbasin'].values, mapping={'var_id':'Rank','dim_id':'gru'})

        # Checking if 'Rank' is monotonic from 1 to n with increament of one
        diff = ntopo['Rank'][:-1] - ntopo['Rank'][1:]
        is_rank_sorted = ntopo['Rank'].is_monotonic_increasing and (ntopo['Rank'].max() == len(ntopo))
        
        # set the subbasin ID as 
        ntopo.set_index('subbasin', inplace=True)

        
        # read GRU and get the dimnesion of GRU
        GRU_num = len(GRU)

        # get the GRU Fract and and assing the 
        flipped_GRU_Frac_mapping = {v: k for k, v in GRU_Frac_mapping.items()}
        GRU_Frac.set_index('subbasin', inplace=True)
        GRU_Frac.reindex(ntopo.index, inplace=True)
        if len(GRU_Frac) != len (ntopo):
            sys.exit('The id in network topology')
        
        # rename the varibales based on var mapping
        if ntopo_vars_mapping is not None:
            ntopo_vars_mapping_flipped = 
            ntopo = ntopo.rename_vars (ntopo_vars_mapping_flipped)
        
        # rename dims
        if ntopo_rename_dims is not None:
            ntopo = ntopo.rename_dims (ntopo_rename_dims)
        
        # assign unit if not provided
        # to be populated based on easymore utility
        
        # change unit to
        # to be populated based on easymore utility
        
        # rename the variables for GRU based on var mapping
        if GRU_vars_mapping if not None:
            GRU_vars_mapping_flipped = 
            GRU = GRU.rename_vars (GRU_vars_mapping_flipped)
            
        # reorder GRU based on IDs in ntopo that are RANK sensitive
        
        # assigning local attributes for each variable
        if attr_local:
            for var, val in attr_local.items():
                for attr, desc in val.items():
                    ddb[var].attrs[attr] = desc

        # assigning global attributes for `ddb`
        if attr_global:
            for attr, desc in attr_global.items():
                ddb.attrs[attr] = desc
        
            
        
        
        # return dbb
        return ddb
    

def MESH_parameters_CLASS(land_cover = 'CEC',
                          eixsting_land_cover = [1,2],
                          outfile = 'MESH_parameters_CLASS.ini',
                          header='header', 
                          footer='footer'):
    
    keys = [str(item) for item in eixsting_land_cover]
    
    from .MESH_default_dict import CEC
    
    merged_data = ""
    
    if header is not None:
        if header.lower() == 'header':
            data = CEC.get(header)
            if data:
                # first replace the temp_land_cover_number with number of land cover from existing land cover
                data = data.replace('temp_land_cover_number', str(len(eixsting_land_cover)))
                merged_data += f"{data}\n"
        else:
            merged_data += f"{header}\n"
    
    for key in keys:
        data = CEC.get(key)
        if data:
            merged_data += f"\n{data}\n"
            
    if footer is not None:
        data = CEC.get(footer)
        if data:
            merged_data += f"\n{data}"
            
    # Save the formatted string to a text file
    with open(outfile, "w") as file:
        file.write(merged_data)
            


def MESH_parameters_hydrology(land_cover = 'CEC',
                              eixsting_land_cover = [1, 2],
                              outfile = 'MESH_parameters_hydrology.ini'):
    
    data_text = {'string': """
2.0: MESH Hydrology parameters input file (Version 2.0)
##### Option Flags #####
----#
    0 														# Number of option flags
##### Channel routing parameters per river class #####
-------#
5 															# Number of channel routing parameters
WF_R2          0.210    1.713    0.206    0.498    0.410 	#only used with old routing
R2N            0.050    0.050    0.050    0.050    0.050    #only used with new routing
R1N            0.119    0.119    0.119    0.119    0.119 	#only used with new routing
PWR            1.361    1.361    1.361    1.361    1.361	#only used with BASEFLOWFLAG wf_lzs
FLZ            4.2E-05  4.2E-05  4.2E-05  4.2E-05  4.2E-05	#only used with BASEFLOWFLAG wf_lzs
##### GRU class independent hydrologic parameters #####     # 10comment line 13                                                           | *
-------#                                                    # 11comment line 14                                                           | *
       0 						    						# Number of GRU independent hydrologic parameters
##### GRU class dependent hydrologic parameters #####       # 18comment line 16                                                           | *
-------#                                                    # 19comment line 17                                                           | *
       4                                                    # 21Number of GRU dependent hydrologic parameters     ??????                  | I8
!name   
"""}
    
    
    # Save the formatted string to a text file
    with open(outfile, "w") as file:
        file.write(data_text.get('string'))
    
    param = {
    "!name": ['NForest(C)', 'NForest(C)', 'DForest(U)', 'DForest(U)', 'DForest(U)', 'MForest(C)', 'Shrublandi(C)', 'Shrublandi(C)', 'Grassland(C)', 'Grassland(C)', 'Grassland(C)', 'Grassland(C)', 'Grassland(C)', 'Wetland(C)', 'Cropland(U)', 'Barrenland(C)', 'Urban(C)', 'Water(C)', 'SnowIce(C)'],
    "ZSNL": [0.134, 0.134, 0.134, 0.134, 0.134, 0.172, 0.578, 0.578, 0.257, 0.257, 0.257, 0.257, 0.257, 0.083, 0.210, 0.100, 0.35, 0.11, 0.05],
    "ZPLS": [0.109, 0.109, 0.109, 0.109, 0.109, 0.122, 0.051, 0.051, 0.09, 0.090, 0.090, 0.090, 0.090, 0.090, 0.134, 0.130, 0.09, 0.09, 0.160],
    "ZPLG": [0.312, 0.312, 0.312, 0.312, 0.312, 0.223, 0.130, 0.130, 0.26, 0.26, 0.26, 0.26, 0.26, 0.260, 0.134, 0.130, 0.26, 0.26, 0.160],
    "IWF": [1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 0, 1.000, 1.000, 1.000, 0, 1.000]}
    
    indices_to_include = [x - 1 for x in eixsting_land_cover]
    
    for key, values in param.items():
        # Extract values at specified indices
        selected_values = [values[i] for i in indices_to_include]

        # Convert values to a formatted string
        data_text = f"{key}   {'      '.join(map(str, selected_values))}\n"

        # Save the formatted string to a text file
        with open(outfile, "a") as file:
            file.write(data_text)


def MESH_Dranage_database (ntopo,
                           GRU,
                           GRU_Frac,
                           ntopo_mapping = {'subbasin': 'COMID',\
                                            'ChnlSlope': 'slope',\
                                            'ChnlLength': 'length',\
                                            'Rank': 'Rank',\
                                            'Next': 'Next',\
                                            'GridArea': 'unitarea',\
                                            'lat': 'latitude',\
                                            'lon': 'longitude'},
                           GRU_mapping = {'combination':'combinations'}
                           GRU_Frac_mapping = {'subbasin': 'COMID'}):
    
    # read ntopo check the rank frist and assign the id as index
    flipped_ntopo_mapping = {v: k for k, v in ntopo_mapping.items()}

    # Renaming columns based on flipped values
    ntopo.rename(columns=flipped_ntopo_mapping, inplace=True)
    
    # 
    ntopo.sort_values(by='Rank', inplace=True)

    # Checking if 'Rank' is sorted properly and equal to the number of rows
    is_rank_sorted = ntopo['Rank'].is_monotonic_increasing and (ntopo['Rank'].max() == len(ntopo))
    
    # convert the index to ID or subbasin
    ntopo.set_index('subbasin', inplace=True)
    
    
    # read GRU and get the dimnesion of GRU
    GRU_num = len(GRU)
    
    
    # get the GRU Fract and and assing the 
    flipped_GRU_Frac_mapping = {v: k for k, v in GRU_Frac_mapping.items()}
    GRU_Frac.set_index('subbasin', inplace=True)
    GRU_Frac.reindex(ntopo.index, inplace=True)
    if len(GRU_Frac) != len (ntopo):
        sys.exit('The id in network topology')
    
    # convert to xarray obsect and return
    ntopo_ds = ntopo.to_xarray()
    
    # convert the GRU Fract to data array
    GRU['combination'].to_xarray()
    
    
    # 
    
    


def MESH_example_forcing (DDB):
    
    
    
    dimensions:
	subbasin = 453 ;
	gru = 20 ;
variables:
	int64 subbasin(subbasin) ;
	double ChnlSlope(subbasin) ;
		ChnlSlope:_FillValue = NaN ;
		ChnlSlope:long_name = "Segment slope" ;
		ChnlSlope:grid_mapping = "crs" ;
		ChnlSlope:coordinates = "lon lat time" ;
		ChnlSlope:units = "dimensionless" ;
	double ChnlLength(subbasin) ;
		ChnlLength:_FillValue = NaN ;
		ChnlLength:long_name = "Segment length" ;
		ChnlLength:grid_mapping = "crs" ;
		ChnlLength:coordinates = "lon lat time" ;
		ChnlLength:units = "meter" ;
	int Rank(subbasin) ;
		Rank:_FillValue = -1 ;
		Rank:standard_name = "Rank" ;
		Rank:long_name = "Element ID" ;
		Rank:grid_mapping = "crs" ;
		Rank:coordinates = "lon lat time" ;
		Rank:units = "dimensionless" ;
	int Next(subbasin) ;
		Next:_FillValue = -1 ;
		Next:standard_name = "Next" ;
		Next:long_name = "Receiving ID" ;
		Next:grid_mapping = "crs" ;
		Next:coordinates = "lon lat time" ;
		Next:units = "dimensionless" ;
	double GRU(subbasin, gru) ;
		GRU:_FillValue = -1. ;
		GRU:standard_name = "GRU" ;
		GRU:long_name = "Group Response Unit" ;
		GRU:coordinates = "lon lat time" ;
		GRU:units = "dimensionless" ;
	double GridArea(subbasin) ;
		GridArea:_FillValue = NaN ;
		GridArea:long_name = "HRU Area" ;
		GridArea:grid_mapping = "crs" ;
		GridArea:coordinates = "lon lat time" ;
		GridArea:units = "meter ** 2" ;
	string LandUse(gru) ;
		LandUse:standard_name = "Landuse classification name" ;
		LandUse:units = "dimensionless" ;
	double lat(subbasin) ;
		lat:_FillValue = NaN ;
		lat:standard_name = "latitude" ;
		lat:units = "degrees_north" ;
		lat:axis = "X" ;
	double lon(subbasin) ;
		lon:_FillValue = NaN ;
		lon:standard_name = "longitude" ;
		lon:units = "degrees_east" ;
		lon:axis = "Y" ;
	int64 crs ;
		crs:grid_mapping_name = "latitude_longitude" ;
		crs:longitude_of_prime_meridian = 0. ;
		crs:semi_major_axis = 6378137. ;
		crs:inverse_flattening = 298.257223563 ;

// global attributes:
		:author = "University of Calgary" ;
		:license = "GNU General Public License v3 (or any later version)" ;
		:purpose = "Create a drainage database .nc file for MESH" ;
		:featureType = "point" ;
        