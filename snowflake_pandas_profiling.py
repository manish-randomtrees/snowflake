# from fastapi import FastAPI
# import uvicorn
from pydantic import BaseModel
from typing import List
import snowflake.connector as sf
from snowflake.snowpark.session import Session
from snowflake.connector.pandas_tools import write_pandas
import snowflake.snowpark.functions as F
from snowflake.snowpark import types as T
import snowflake.snowpark
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score
from sklearn import datasets
# import mlxtend
# from mlxtend.feature_selection import ColumnSelector
# import plotly.express as px
from snowflake.snowpark.functions import udf
from PIL import Image
import csv
from sklearn.metrics import mean_squared_error, r2_score
#import seaborn as sns
import json
import pandas_profiling as pp
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.udf import DataType as DT
from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark import types as T


def create_session_object():
    connection_parameters = {
   "account": 'nq81064.us-east4.gcp',
   "user": 'MAK',
   "password": 'Rocky1234',   
   "database": 'SNOWML',
   "schema": 'DEMO',
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH"
    }
    session = Session.builder.configs(connection_parameters).create()
    session.add_packages('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy','pandas-profiling')
    return session

session = create_session_object()

query = "create or replace stage models" +\
        " directory = (enable = true)" +\
        " copy_options = (on_error='skip_file')"
        
session.sql(query).collect()

query = "create or replace stage udf" +\
        " copy_options = (on_error='skip_file')"
        
session.sql(query).collect()

def train_model(session:snowflake.snowpark.Session) -> dict:
        snowdf = session.table("HOUSING_DATA").to_pandas()
        profile = pp.ProfileReport(snowdf)
        json_data = profile.to_json()
        d = json.loads(json_data)
        return d

train_model_sp = sproc(train_model,replace=True)

train_model_sp()

print(train_model_sp())

