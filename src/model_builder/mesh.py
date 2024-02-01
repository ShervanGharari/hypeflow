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

from easymore import Utility as esmrut
from easymore import Easymore




class MESHWorkflow(object):
    """
    Main Workflow class of HYPE


    Attributes
    ----------


    Parameters
    ----------


    """
    def __init__(self) -> None:
        print('MESH workflow initiated')
    
    
    def MESH_Dranage_database (self,
                               ntopo,
                               ntopo_mapping,
                               ntopo_rename_dims,
                               GRU,
                               GRU_mapping,
                               GRU_rename_dims):

        #
        from .MESH_default_dict import ddb_local_attrs_default, default_attrs

        # read ntopo check the rank frist and assign the id as index
        flipped_ntopo_mapping = {v: k for k, v in ntopo_mapping.items()}

        # Renaming columns based on flipped values
        ntopo = ntopo.rename_vars(flipped_ntopo_mapping)

        # 
        flipped_ntopo_rename_dims = {v: k for k, v in ntopo_rename_dims.items()}
        ntopo = ntopo.swap_dims(flipped_ntopo_rename_dims)

        # sort based on rank to make sure rank is from 1 to n
        ntopo = esmrut.reorder(ntopo, ntopo['Rank'].values, mapping={'var_id':'Rank','dim_id':'gru'})

        # Checking if 'Rank' is monotonic from 1 to n with increament of one
        ntopo_Rank = np.array(ntopo['Rank'].values)
        if not (np.all(np.diff(ntopo_Rank) == 1) and ntopo_Rank[0]==1):
            sys.exit('Rank is not monotonic')

        # rename dimensions
        flipped_GRU_rename_dims = {v: k for k, v in GRU_rename_dims.items()}
        GRU = GRU.swap_dims(flipped_GRU_rename_dims)

        # read ntopo check the rank frist and assign the id as index
        flipped_GRU_mapping = {v: k for k, v in GRU_mapping.items()}

        # Renaming columns based on flipped values
        GRU = GRU.rename_vars(flipped_GRU_mapping)

        # sort the GRU based on existing ids in ntopo (or RANK)
        GRU = esmrut.reorder(GRU, ntopo['subbasin'].values, mapping = {'var_id':'subbasin','dim_id':'subbasin'})

        # pass some variables
        ntopo['GRU'] = GRU['fraction']
        ntopo['GRU_name'] = GRU['GRU_name']

        # add crs and its attributes
        ntopo['crs'] = 1
        for k in default_attrs:
            for attr, desc in default_attrs[k].items():
                ntopo[k].attrs[attr] = desc

        # adjust local attributes of common variables
        for k in ddb_local_attrs_default:
            for attr, desc in ddb_local_attrs_default[k].items():
                ntopo[k].attrs[attr] = desc

        # return dbb
        return ntopo
    

    def MESH_parameters_CLASS(self,
                              land_cover = 'CEC',
                              soil_type = 'USDA',
                              existing_land_cover = [1,2],
                              existing_soil_type = [1,2],
                              outfile = 'MESH_parameters_CLASS.ini'):

        # make the model agnostic 
        from .MESH_default_dict import CEC, USDA, extra_CLASS_param, tail_CLASS_param, head_CLASS_param

        merged_data = ""

        # read the header and replace the temp with num of GRU
        temp = head_CLASS_param.get(1)
        temp = temp.replace('GRU_TOTAL_NO', str(len(existing_land_cover)))
        merged_data = merged_data + temp + '\n'


        # loo over the soil and land cover
        for m in np.arange (len (existing_land_cover)):

            # get and add land cover
            temp1 = CEC.get(existing_land_cover[m])
            temp1 = temp1.replace('GRU_NO', str(m+1))
            temp1 = temp1.replace('GRU_NAME', 'soil_'+str(existing_soil_type[m]))

            # get and add soil type
            temp2 = USDA.get(existing_soil_type[m])


            merged_data = merged_data + temp1 + temp2+ '\n'

            # get the extra param and ass
            merged_data = merged_data + extra_CLASS_param.get(1) + '\n'


        #
        merged_data = merged_data + tail_CLASS_param.get(1)

    #     if header is not None:
    #         if header.lower() == 'header':
    #             data = CEC.get(header)
    #             if data:
    #                 # first replace the temp_land_cover_number with number of land cover from existing land cover
    #                 data = data.replace('temp_land_cover_number', str(len(eixsting_land_cover)))
    #                 merged_data += f"{data}\n"
    #         else:
    #             merged_data += f"{header}\n"

    #     for key in keys:
    #         data = CEC.get(key)
    #         if data:
    #             merged_data += f"\n{data}\n"

    #     if footer is not None:
    #         data = CEC.get(footer)
    #         if data:
    #             merged_data += f"\n{data}"

        # Save the formatted string to a text file
        with open(outfile, "w") as file:
            file.write(merged_data)
    
#     def MESH_parameters_CLASS(land_cover = 'CEC',
#                               eixsting_land_cover = [1,2],
#                               outfile = 'MESH_parameters_CLASS.ini',
#                               header='header', 
#                               footer='footer'):

#         keys = [str(item) for item in eixsting_land_cover]

#         from .MESH_default_dict import CEC

#         merged_data = ""

#         if header is not None:
#             if header.lower() == 'header':
#                 data = CEC.get(header)
#                 if data:
#                     # first replace the temp_land_cover_number with number of land cover from existing land cover
#                     data = data.replace('temp_land_cover_number', str(len(eixsting_land_cover)))
#                     merged_data += f"{data}\n"
#             else:
#                 merged_data += f"{header}\n"

#         for key in keys:
#             data = CEC.get(key)
#             if data:
#                 merged_data += f"\n{data}\n"

#         if footer is not None:
#             data = CEC.get(footer)
#             if data:
#                 merged_data += f"\n{data}"

#         # Save the formatted string to a text file
#         with open(outfile, "w") as file:
#             file.write(merged_data)
            


    def MESH_parameters_hydrology(self,
                                  land_cover = 'CEC',
                                  eixsting_land_cover = [1, 19],
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


# def MESH_Dranage_database (ntopo,
#                            GRU,
#                            GRU_Frac,
#                            ntopo_mapping = {'subbasin': 'COMID',\
#                                             'ChnlSlope': 'slope',\
#                                             'ChnlLength': 'length',\
#                                             'Rank': 'Rank',\
#                                             'Next': 'Next',\
#                                             'GridArea': 'unitarea',\
#                                             'lat': 'latitude',\
#                                             'lon': 'longitude'},
#                            GRU_mapping = {'combination':'combinations'}
#                            GRU_Frac_mapping = {'subbasin': 'COMID'}):
    
#     # read ntopo check the rank frist and assign the id as index
#     flipped_ntopo_mapping = {v: k for k, v in ntopo_mapping.items()}

#     # Renaming columns based on flipped values
#     ntopo.rename(columns=flipped_ntopo_mapping, inplace=True)
    
#     # 
#     ntopo.sort_values(by='Rank', inplace=True)

#     # Checking if 'Rank' is sorted properly and equal to the number of rows
#     is_rank_sorted = ntopo['Rank'].is_monotonic_increasing and (ntopo['Rank'].max() == len(ntopo))
    
#     # convert the index to ID or subbasin
#     ntopo.set_index('subbasin', inplace=True)
    
    
#     # read GRU and get the dimnesion of GRU
#     GRU_num = len(GRU)
    
    
#     # get the GRU Fract and and assing the 
#     flipped_GRU_Frac_mapping = {v: k for k, v in GRU_Frac_mapping.items()}
#     GRU_Frac.set_index('subbasin', inplace=True)
#     GRU_Frac.reindex(ntopo.index, inplace=True)
#     if len(GRU_Frac) != len (ntopo):
#         sys.exit('The id in network topology')
    
#     # convert to xarray obsect and return
#     ntopo_ds = ntopo.to_xarray()
    
#     # convert the GRU Fract to data array
#     GRU['combination'].to_xarray()
    
    
#     # 
    
    


# def MESH_example_forcing (DDB):