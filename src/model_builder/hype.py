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


class HYPEWorkflow(object):
    """
    Main Workflow class of HYPE


    Attributes
    ----------


    Parameters
    ----------


    """
    # main constructor
    # def __init__(self) -> None:
        
        
    def GeoClass (self,
                  combination,
                  outfile,
                  mapping = {'SLC':'ID','landcover':'LULC','soil':'SOIL TYPE'},
                  commented_lines = '! HYPE GeoClass'):
    
        # rename the dataframe
        flipped_mapping = {v: k for k, v in mapping.items()}

        combination.rename(columns=flipped_mapping, inplace=True)
        
        # order the first two 
        combination = combination[['SLC','landcover','soil']]

        # populate the extra rows
        combination['Main crop cropid'] = 0
        combination['Second crop cropid'] = 0
        combination['Crop rotation group'] = 0
        combination['Vegetation type'] = 1
        combination['Special class code'] = 0
        combination['Tile depth'] = 0
        combination['Stream depth'] = 2.296
        combination['Number of soil layers'] = 3
        combination['Soil layer depth 1'] = 0.091
        combination['Soil layer depth 2'] = 0.493
        combination['Soil layer depth 3'] = 2.296

        # write the file
        # Open the file in write mode
        with open(outfile, 'w') as file:
            # Write the commented lines
            #for line in commented_lines:
            file.write(commented_lines + '\n')

        # writing the `GeoClass.txt` file
        with open(outfile, 'a') as file:
                combination.to_csv(file, sep='\t', index=False, header=False)
    
    def GaoData (self,
                 *dfs: pd.DataFrame,
                 df_mappings: Union[Dict[str, Dict[str, str]], None] = None,
                 outfile):
    
        # turn input into list
        dfs = list(dfs)

        # Loop through each DataFrame
        for idx, df in enumerate(dfs, start=1):

            # get the mapping for a given df
            mapping = df_mappings.get(f'df{idx}', {})
            ID = mapping.get('id', None)
            rename_dict = mapping.get('rename_dict', {})

            # Set the ID column to index for the DataFrame
            if ID is not None:
                df.index = df[ID].astype(int)
                df.drop(columns=[ID], inplace=True)
                df = df.sort_index()

            # rename the df and keep renamed columns
            if rename_dict:
                rename_dict_flip = {v: k for k, v in rename_dict.items()}
                df = self.rename_columns (df,
                                          rename_dict = rename_dict_flip)

            # update the dataframe
            dfs[idx - 1] = df

        # Compare the indexes
        index_comparison = all(dfs[i].index.equals(dfs[i + 1].index) for i in range(len(dfs) - 1))
        if index_comparison:
            print("The indexes of all DataFrames are exactly the same with the same order.")
        else:
            sys.exit("The indexes of DataFrames are different or in a different order.")

        # merge all the dfs horizontally
        merged_df  = pd.concat(dfs,axis=1)

        # order based on the upstream area (from upstream to downstream)
        merged_df = merged_df.sort_values(by='up_area').reset_index(drop=True)
        merged_df = merged_df.drop(columns=['up_area'])
        slc_columns = [col for col in merged_df.columns if col.startswith('SLC_')]
        other_columns = ['subid','maindown','area','latitude','longitude','elev_mean','slope_mean','rivlen']
        merged_df = merged_df[other_columns + slc_columns]
        merged_df.to_csv(outfile, sep='\t', index=False)

        # return 
        return merged_df
    
    
    def rename_columns (self,
                        df,
                        rename_dict: Dict[str, str] = None,
                        keep_renamed_only = True):
    
        # initialize column name list
        col_name_list = []

        # loop over the key and values of the rename_dictionary
        for key, value in rename_dict.items():

            if '*' in key:
                # Case: Columns that include the string 'slope'
                if key.startswith('*') and key.endswith('*'):
                    filtered_cols = [col for col in df.columns if key[1:-1] in col]
                # Case: Columns that start with the string 'slope'
                elif key.endswith('*'):
                    filtered_cols = [col for col in df.columns if col.startswith(key[:-1])]
                # Case: Columns that end with the string 'slope'
                elif key.startswith('*'):
                    filtered_cols = [col for col in df.columns if col.endswith(key[1:])]
                else:
                    raise ValueError("Invalid wildcard placement in the key.")
            else:
                # Case: Columns that start with the given key
                filtered_cols = [key]

            if filtered_cols:            
                # loop over each col name and rename it based on 
                for filtered_col in filtered_cols:
                    old_string = filtered_col
                    new_string = old_string.replace(key.replace("*",""), value)
                    df = df.rename(columns={filtered_col:new_string})
                    col_name_list.append(new_string)
            else:
                print('there is no column name called ' + value + ' in the dataframe')
                
        # subset the input dataframe for given renamed columns
        if keep_renamed_only:
            df = df [col_name_list]

        # return
        return df

    def Parameters (self,
                    output_file,
                    soil_infile = None,
                    soil_number = 12,
                    soil_type = "usda",
                    land_cover_infile = None,
                    land_cover_number = 19,
                    land_cover_type = "cec"):

        # 
        from .HYPE_default_dict import General

        # write general
        for dictionary in General.values():
            self.write_dictionary(output_file, dictionary)

        # soil type
        if soil_type.lower() == 'usda':
            #print('inside soil')
            from .HYPE_default_dict import USDA
            #print(USDA)
            # write general
            for dictionary in USDA.values():
                self.write_dictionary(output_file, dictionary, num = soil_number)

        # land cover
        if land_cover_type.lower() == 'cec':
            #print('inside land cover')
            from .HYPE_default_dict import CEC
            #print(CEC)
            # write general
            for dictionary in CEC.values():
                self.write_dictionary(output_file, dictionary, num = land_cover_number)
                
    def write_dictionary(self,output_file, dictionary, num = None):
        with open(output_file, 'a') as file:
            for key, value in dictionary.items():
                if key != 'section_head':
                    if isinstance(value['value'], list):
                        if not num is None: # check the length
                            if num != len(value['value']):
                                sys.exit('the list is not with the same length as soil\
    or land cover for key: '+ key)
                        values_line = '\t'.join(map(str, value['value']))
                        file.write(f"{key}\t{values_line}\t{value['comment']}\n")
                    else:
                        file.write(f"{key}\t{value['value']}\t{value['comment']}\n")
                else:
                    file.write(dictionary['section_head']+"\n")

    def sample_forcing (self,
                        ID,
                        out_folder = './'):
    
    
        # create a smaple forcing file
        import datetime

        # List of column names
        columns = list (ID)

        # Start date and end date
        start_date = datetime.date(2023, 10, 1)
        end_date = start_date + datetime.timedelta(days=9)  # 10 days

        # List of dates for 10 days
        dates = [start_date + datetime.timedelta(days=i) for i in range(10)]

        # Create DataFrame
        df = pd.DataFrame(columns=columns, index=dates)
        df.index = pd.to_datetime(df.index)

        # round values
        df = df.round(3)

        # Pobs
        # name index to data and save as txt
        for col in columns:
            df[col] = np.random.uniform(0, 1, size=len(dates))
        # name index to time
        ds = xr.Dataset({'Pobs': (['time', 'id'], df.values.astype(float))},
                 coords={'time': df.index, 'id': df.columns})
        # save
        df.to_csv(out_folder+'Pobs.txt', sep='\t', na_rep='', index_label='Date', float_format='%.2f')
        if os.path.isfile (out_folder+'Pobs.nc'):
            os.remove (out_folder+'Pobs.nc')
        ds.to_netcdf(out_folder+'Pobs.nc', encoding = {'Pobs':{'_FillValue': -9999.0}})

        # Tobs
        for i, col in enumerate(columns):
            # Generate values from -5 to +5 for each column
            df[col] = [(i % 11) - 5 for i in range(len(dates))]
        # name index to time
        ds = xr.Dataset({'Tobs': (['time', 'id'], df.values.astype(float))},
                 coords={'time': df.index, 'id': df.columns})
        # save
        df.to_csv(out_folder+'Tobs.txt', sep='\t', na_rep='', index_label='Date', float_format='%.2f')
        if os.path.isfile (out_folder+'Tobs.nc'):
            os.remove (out_folder+'Tobs.nc')
        ds.to_netcdf(out_folder+'Tobs.nc', encoding = {'Tobs':{'_FillValue': -9999.0}})

        # TMAXobs
        for col in columns:
            df[col] = df[col]+1
        # name index to time
        ds = xr.Dataset({'TMAXobs': (['time', 'id'], df.values.astype(float))},
                 coords={'time': df.index, 'id': df.columns})
        # save
        df.to_csv(out_folder+'TMAXobs.txt', sep='\t', na_rep='', index_label='Date', float_format='%.2f')
        if os.path.isfile (out_folder+'TMAXobs.nc'):
            os.remove (out_folder+'TMAXobs.nc')
        ds.to_netcdf(out_folder+'TMAXobs.nc', encoding = {'TMAXobs':{'_FillValue': -9999.0}})

        # TMINobs
        for col in columns:
            df[col] = df[col]-2
        # name index to time
        ds = xr.Dataset({'TMINobs': (['time', 'id'], df.values.astype(float))},
                 coords={'time': df.index, 'id': df.columns})
        # save
        df.to_csv(out_folder+'TMINobs.txt', sep='\t', na_rep='', index_label='Date', float_format='%.2f')
        if os.path.isfile (out_folder+'TMINobs.nc'):
            os.remove (out_folder+'TMINobs.nc')
        ds.to_netcdf(out_folder+'TMINobs.nc', encoding = {'TMINobs':{'_FillValue': -9999.0}})

#     @property
#     def coords(self):
#         # calculate centroid latitude and longitude values
#         return utility.extract_centroid(gdf=self.cat,
#                                         obj_id=self.main_id)

#     @property
#     def forcing_files(self):
#         pattern = r"\.nc\*?|\.nc"
#         if re.search(pattern, self._forcing_path):
#             if glob.glob(self._forcing_path):
#                 return self._forcing_path
#         else:
#             _path = os.path.join(self._forcing_path, '*.nc*')
#             if glob.glob(_path):
#                 return _path

#     @classmethod
#     def from_dict(
#         cls: 'MESHWorkflow',
#         init_dict: Dict = {},
#     ) -> 'MESHWorkflow':
#         """
#         Constructor to use a dictionary to instantiate
#         """
#         if len(init_dict) == 0:
#             raise KeyError("`init_dict` cannot be empty")
#         assert isinstance(init_dict, dict), "`init_dict` must be a `dict`"

#         return cls(**init_dict)

#     @classmethod
#     def from_json(
#         cls: 'MESHWorkflow',
#         json_str: str,
#     ) -> 'MESHWorkflow':
#         """
#         Constructor to use a loaded JSON string
#         """
#         # building customized MESHWorkflow's JSON string decoder object
#         decoder = json.JSONDecoder(object_hook=MESHWorkflow._json_decoder)
#         json_dict = decoder.decode(json_str)
#         # return class instance
#         return cls.from_dict(json_dict)

#     @classmethod
#     def from_json_file(
#         cls: 'MESHWorkflow',
#         json_file: 'str',
#     ) -> 'MESHWorkflow':
#         """
#         Constructor to use a JSON file path
#         """
#         with open(json_file) as f:
#             json_dict = json.load(f,
#                                   object_hook=MESHWorkflow._easymore_decoder)

#         return cls.from_dict(json_dict)

#     @staticmethod
#     def _env_var_decoder(s):
#         """
#         OS environmental variable decoder
#         """
#         # RE patterns
#         env_pat = r'\$(.*?)/'
#         bef_pat = r'(.*?)\$.*?/?'
#         aft_pat = r'\$.*?(/.*)'
#         # strings after re matches
#         e = re.search(env_pat, s).group(1)
#         b = re.search(bef_pat, s).group(1)
#         a = re.search(aft_pat, s).group(1)
#         # extract environmental variable
#         v = os.getenv(e)
#         # return full: before+env_var+after
#         if v:
#             return b+v+a
#         return s

#     @staticmethod
#     def _json_decoder(obj):
#         """
#         Decoding typical JSON strings returned into valid Python objects
#         """
#         if obj in ["true", "True", "TRUE"]:
#             return True
#         elif obj in ["false", "False", "FALSE"]:
#             return False
#         elif isinstance(obj, str):
#             if '$' in obj:
#                 return MESHWorkflow._env_var_decoder(obj)
#         elif isinstance(obj, dict):
#             return {k: MESHWorkflow._json_decoder(v) for k, v in obj.items()}
#         return obj