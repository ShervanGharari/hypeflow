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
    def __init__(
        self,
        ntopo: str = None,
        ntopo_mapping = Dict[str, str] = {'id':'id','next_id':'next_id','area':'area',\
                                          'uparea':'uparea','length':'length'},
        landcover: str = None,
        landcover_classes: Dict[int, str] = None,
        landcover_mapping: Dict[str, str] = {'id':'id', 'prefix': 'frac_'},
        soiltype: str = None,
        soiltype_classes: Dict[str, str] = None,
        soiltype_mapping: Dict[str, str] = {'id':'id', 'prefix': 'frac_'},
        outlet_value: int = -9999
    ) -> None:
        """
        Main constructor of HYPEworkflow
        """
            
        # check dictionary dtypes
        _dict_items = [ntopo_mapping,
                       landcover_classes,
                       landcover_mapping,
                       soiltype_classes,
                       soiltype_mapping]
        for item in _dict_items:
            if not isinstance(item, dict) and (item is not None):
                raise TypeError(f"`{item}` must be of type dict")
                
        # read the ntopo files, keep the needed columns and then rename then based on 
        # mapping
        ntopo = pd.read_csv(ntopo)
        rename_dict = {value: key for key, value in ntopo_mapping.items()}
        columns_to_keep = [col for col in ntopo.columns if col in rename_dict.values()]
        ntopo = ntopo[columns_to_keep]
        ntopo = ntopo.rename(rename_dict)
        
        # get the number and int of each land cover
        landcover_classes_int = []
        for key in landcover_classes.keys():
            try:
                landcover_classes_int.append(int(key))
            except ValueError:
                raise ValueError(f"land cover '{key}' cannot be converted to an integer.")
        
        # load the land cover, get the flex number with prefix
        landcover = pd.read_csv(landcover)
        if landcover_mapping['prefix'] != '': # get the column names starting with fract
            prefix = landcover_mapping['prefix']
            landcover_int = [int(re.search(rf'{prefix}(\d+)', col).group(1)) for \ 
                             col in landcover.columns if re.search(rf'{prefix}\d+', col)]
            # check if the set of numbers are the same as the index provided in the 
            if not (set (landcover_int) <= set (landcover_classes_int)):
                sys.exit('Provided land cover code in fraction file do not eixstis in land cover classes')
        
        # landcover id rename
        landcover ['id'] = landcover [landcover_mapping['id']]
        
        
        # get the number of soil
        soiltype_classes_int = []
        for key in soiltype_classes.keys():
            try:
                soiltype_classes_int.append(int(key))
            except ValueError:
                raise ValueError(f"soil type '{key}' cannot be converted to an integer.")
                
        # get the majority and check if this is all in the
        soiltype = pd.read_file(soiltype)
        try:
            soiltype[soiltype_mapping['mojority']] = soiltype[soiltype_mapping['mojority']].astype(int)
        except ValueError as e:
            raise ValueError(f"Error: {e}. The soil type column contains non-integer values.")
        
        # check the soil type
        if not (set (np.unique(soiltype[soiltype_mapping['mojority']])) <= set (soiltype_classes_int)):
            sys.exit('the provided majority soil is not in the soil class')
            
        
        # check the IDs of ntopo and slice soil, land cover info for that
        if (set(ntopo['id'].astype(int)) > set (soiltype['id'].astype(int))) or /
        (set(ntopo['id'].astype(int)) > set (landcover['id'].astype(int))):
            sys.exit('There are ids in ntopo that are not provided in soiltype or landcover files')
            
        # subset the land cover and soil for ntopo ids
            
        
        
        
    
    def GeoClass ()

    @property
    def coords(self):
        # calculate centroid latitude and longitude values
        return utility.extract_centroid(gdf=self.cat,
                                        obj_id=self.main_id)

    @property
    def forcing_files(self):
        pattern = r"\.nc\*?|\.nc"
        if re.search(pattern, self._forcing_path):
            if glob.glob(self._forcing_path):
                return self._forcing_path
        else:
            _path = os.path.join(self._forcing_path, '*.nc*')
            if glob.glob(_path):
                return _path

    @classmethod
    def from_dict(
        cls: 'MESHWorkflow',
        init_dict: Dict = {},
    ) -> 'MESHWorkflow':
        """
        Constructor to use a dictionary to instantiate
        """
        if len(init_dict) == 0:
            raise KeyError("`init_dict` cannot be empty")
        assert isinstance(init_dict, dict), "`init_dict` must be a `dict`"

        return cls(**init_dict)

    @classmethod
    def from_json(
        cls: 'MESHWorkflow',
        json_str: str,
    ) -> 'MESHWorkflow':
        """
        Constructor to use a loaded JSON string
        """
        # building customized MESHWorkflow's JSON string decoder object
        decoder = json.JSONDecoder(object_hook=MESHWorkflow._json_decoder)
        json_dict = decoder.decode(json_str)
        # return class instance
        return cls.from_dict(json_dict)

    @classmethod
    def from_json_file(
        cls: 'MESHWorkflow',
        json_file: 'str',
    ) -> 'MESHWorkflow':
        """
        Constructor to use a JSON file path
        """
        with open(json_file) as f:
            json_dict = json.load(f,
                                  object_hook=MESHWorkflow._easymore_decoder)

        return cls.from_dict(json_dict)

    @staticmethod
    def _env_var_decoder(s):
        """
        OS environmental variable decoder
        """
        # RE patterns
        env_pat = r'\$(.*?)/'
        bef_pat = r'(.*?)\$.*?/?'
        aft_pat = r'\$.*?(/.*)'
        # strings after re matches
        e = re.search(env_pat, s).group(1)
        b = re.search(bef_pat, s).group(1)
        a = re.search(aft_pat, s).group(1)
        # extract environmental variable
        v = os.getenv(e)
        # return full: before+env_var+after
        if v:
            return b+v+a
        return s

    @staticmethod
    def _json_decoder(obj):
        """
        Decoding typical JSON strings returned into valid Python objects
        """
        if obj in ["true", "True", "TRUE"]:
            return True
        elif obj in ["false", "False", "FALSE"]:
            return False
        elif isinstance(obj, str):
            if '$' in obj:
                return MESHWorkflow._env_var_decoder(obj)
        elif isinstance(obj, dict):
            return {k: MESHWorkflow._json_decoder(v) for k, v in obj.items()}
        return obj
    
