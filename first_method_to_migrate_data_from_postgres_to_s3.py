import psycopg2
import pandas as pd
import boto3
import boto3
from botocore.exceptions import NoCredentialsError
#establishing the connection
databasename = "manishpost"
username = "manishpost"
passwordname = "aaaa"
hostname = "localhost"
portname = "5432"
tablename = "users"
conn = psycopg2.connect(   database=databasename, user=username, password=passwordname, host= hostname, port= portname)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#Executing an MYSQL function using the execute() method# f"Hello, {name}. You are {age}."
cursor.execute(f"select * from {tablename}")# Fetch a single row using fetchone() method.
data = cursor.fetchall()
data = pd.DataFrame(data)# num_fields = len(cursor.description)
field_names = [i[0] for i in cursor.description]
data.columns = field_names
df = pd.DataFrame(data)
df.to_csv(f'{tablename}.csv')
conn.close()# s3 = boto3.client('s3', aws_access_key_id='83OTjiwlDSw67zFrvG2HT+IawR2pax9Rip+YfAVP', aws_secret_access_key='AKIA5GYGG7C7WIWDRWPW')
ACCESS_KEY = 'AKIA5GYGG7C7WIWDRWPW'
SECRET_KEY = '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'
def upload_to_aws(local_file, bucket, s3_file):    
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, 
    aws_secret_access_key=SECRET_KEY)    
    try:
        s3.upload_file(local_file, bucket, s3_file)        
        print("Upload Successful")        
        return True    
    except FileNotFoundError:        
        print("The file was not found")        
        return False    
    except NoCredentialsError:        
        print("Credentials not available")        
        return False
uploaded = upload_to_aws('users.csv', 'dfast', 'users.csv')
