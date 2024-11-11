import os
import glob
import json
import pandas as pd
from pathlib import Path
# DICOM centric packages
import pydicom

def fxn_emiz_load_dicom_in_pandas(var_dicom_directory, var_pandas_dataframe):
  """
  Recursively load DICOM files

  This function recursively loads all DICOM files in a given
  directory, into an existing Pandas DataFrame

  Parameters
  ----------
  var_dicom_directory: str
      Root directory comprising of DICOM files to load in DataFrame

  var_pandas_dataframe: pandas.core.frame.DataFrame
      Pandas DataFrame to append DICOM metadata to

  Returns
  -------
  pandas.core.frame.DataFrame
      Modified DataFrame with additional data added

  Examples
  --------
  def fxn_emiz_load_dicom_in_pandas(".", var_uths_pandas_dataframe)
  """
  for var_dicom_file in Path(var_dicom_directory).rglob("*.dcm"):
    #print(var_dicom_file)
    var_dicom_file = pydicom.dcmread(var_dicom_file)
    var_pandas_dataframe = var_pandas_dataframe.append(
        {
            var_key:var_value for var_key in var_dicom_file.to_json_dict().keys() if var_key not in ["7FE00010"] for var_value in var_dicom_file.to_json_dict()[var_key].get("Value", [])
            },
            ignore_index=True)
  return var_pandas_dataframe
