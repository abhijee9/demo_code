import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import base64
import json
import subprocess
import os
import pgpy
from pgpy import PGPKey
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


kms_client = boto3.client('kms')
secret_client = boto3.client('secretsmanager')

s3_resource = boto3.resource('s3')
# Replace these with your actual values
#encrypted_file_path = "/tmp/encrypted/abhijeet.txt"
#decrypted_file_path = "/tmp/decrypted/abhijeet.txt"
#dowland_file_path = "/tmp/downloading/abhijeet.txt"
encrypted_dir = "/tmp/encrypted"
decrypted_dir = "/tmp/decrypted"
download_dir = "/tmp/downloading"
os.mkdir(encrypted_dir)
os.mkdir(decrypted_dir)
os.mkdir(download_dir)
source_bucket = s3_resource.Bucket("954500607455-source-dev")
destination_bucket = s3_resource.Bucket("954500607455-destination-dev")
#change the filename that u want to encrypt 
# to encrypt ---> encrypt = 1. decrypt = 0
# to decrypt ---> encrypt = 0. decrypt = 1
#to both     ---> encrypt = 1. decrypt = 1
encrypt = 1
decrypt = 0
filename = "T.FDL.FACT_ANNUITY_CONTRACT_DETAIL_SNAPSHOT_QUARTERLY_EXTRACT.7.20240223093957.dat"

s3_upload_location_for_unencrypted = "unencrypted"
s3_upload_location_for_encrypted="encrypted"
if(encrypt):
    s3_key = s3_upload_location_for_unencrypted +'/'+ filename
else:
    s3_key = s3_upload_location_for_encrypted +'/'+ filename

decrypted_file_path = decrypted_dir+'/'+filename
encrypted_file_path = encrypted_dir +'/'+ filename
dowland_file_path = download_dir +'/'+ filename


s3_resource.meta.client.download_file("954500607455-source-dev",s3_key,dowland_file_path)


secret_name = "arn:aws:secretsmanager:us-east-1:954500607455:secret:test1-hIM1uV"
private_key_string = secret_client.get_secret_value(SecretId=secret_name)
private_key_string = private_key_string['SecretString']
private_key_string = private_key_string.encode('utf-8')
print("private_key_string", private_key_string)

secret_name = "arn:aws:secretsmanager:us-east-1:954500607455:secret:test2-Nf0sxn"
public_key_string = secret_client.get_secret_value(SecretId=secret_name)
public_key_string = public_key_string['SecretString']

public_key_string = public_key_string.encode('utf-8')
print("public_key_string", public_key_string)

# Read the file to encrypt

passphrase="abhijeet"
private_key, _ = PGPKey.from_blob(private_key_string)
public_key, _ = PGPKey.from_blob(public_key_string)

if encrypt == 1:
    file_message = pgpy.PGPMessage.new(dowland_file_path, file=True)
    encrypted_data = public_key.encrypt(file_message)
    with open(encrypted_file_path, "wb") as f:
        f.write(bytes(encrypted_data))
    
    upload_command = f"aws s3 cp  {encrypted_file_path} s3://954500607455-destination-dev/encrypted/"
    subprocess.run(upload_command, shell=True, check=True)
        
if decrypt == 1:      
    with open(dowland_file_path, "rb") as f:
        encrypted_data = f.read()
        encrypted_data = pgpy.PGPMessage.from_blob(encrypted_data)
        with private_key.unlock("ltohub"):
            decrypted_data = private_key.decrypt(encrypted_data).message
            #decrypted_data = decrypted_data.encode('utf-8')
            with open(decrypted_file_path, "wb") as f:
                f.write(decrypted_data)
    upload_command = f"aws s3 cp  {decrypted_file_path} s3://954500607455-destination-dev/decrypted/"
    subprocess.run(upload_command, shell=True, check=True)
         
job.commit()

