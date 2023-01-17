import os
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from snowflake.snowpark.session import Session


train_df = pd.read_csv("diabetes.csv")
for i in os.listdir('/home/manish/Desktop/new folder/upload dataset to snowflake'):
    train_data_name = i
    print(i)
cnn = sf.connect(user="manishreddy",password="S@ndhya03",account="ce01427.central-india.azure",warehouse="SQL_CHALLENGE",database="SQL_CHALLENGE",schema="PUBLIC")
cs = cnn.cursor()
train_data_name = "DIABETES"
columns = train_df.columns
columns = ['_'.join(i.split(' ')) for i in columns]
columns = ['_'.join(i.split('-')) for i in columns]
columns = ['_'.join(i.split('/')) for i in columns]
train_df.columns = columns
train_df.columns = train_df.columns.str.upper()
print(train_df.columns)
cols = []
for i in train_df.columns:
    print(train_df[i].dtype)
    if(train_df[i].dtype=='object'):
        cols.append(i + ' varchar')
    if(train_df[i].dtype=='int64' or train_df[i].dtype == 'int32' or train_df[i].dtype == 'int16' or train_df[i].dtype == 'int8'):
        cols.append(i + ' float')
    if(train_df[i].dtype=='float64'):
        cols.append(i + ' float')
    if(train_df[i].dtype=='datetime64[ns]'):
        tz='UTC'
        train_df[i] = train_df[i].dt.tz_localize(tz)
        cols.append(i + ' datetime')
cols = ', '.join(cols)
#print(cols)
#print(f"""CREATE OR REPLACE TABLE TRAIN_DATA ( {cols},{target_column} float );""")
# cs.execute(f"""CREATE OR REPLACE TABLE TRAIN_DATA( {cols},{target_column} float )""")
cs.execute(f"""CREATE OR REPLACE TABLE {train_data_name}(""" + cols + """)""")
train_df.columns = [x.upper() for x in train_df.columns]

CONNECTION_PARAMETERS = {
   "account": 'nq81064.us-east4.gcp',
   "user": 'MAK',
   "password": 'Rocky1234',
   "database": 'snowml',
   "schema": 'demo',
   "role":'accountadmin',
   "warehouse": 'compute_wh'
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()

snowpark_df = session.write_pandas(train_df, "DEMAND_FORECASTING")

if(__name__=="__main__"):
    uvicorn.run("main:app",reload=True)