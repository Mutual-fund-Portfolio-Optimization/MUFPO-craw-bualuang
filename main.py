from src.scrape_daily_nav import extract_data, fund_url
from src.cookie_collector import cookie
from config import BASE_URL
import numpy as np
import pandas as pd
import json
import boto3
from functools import partial
import os


ec2_client = boto3.client('ec2', region_name='ap-southeast-1')
ssm_client = boto3.client('ssm', region_name='ap-southeast-1')

def stop_Ec2():
    response = ec2_client.stop_instances(InstanceIds=["i-0e05f999b7826eb63"])
    return response

def create_s3_client():
    return boto3.client('s3')

def upload_file_to_s3(s3_client, bucket_name, file_path):
    try:
        s3_client.upload_file(file_path, bucket_name, file_path.split('/')[-1])
        return f"File uploaded to {bucket_name}"
    except Exception as e:
        return f"Error uploading file: {e}"

def upload_file_functional():
    s3_client = create_s3_client()
    upload_func = partial(upload_file_to_s3, s3_client, 's3://mufpo-datalake/craw_fund/')
    return upload_func('out/result.csv')

def main():
    cookie_string = cookie(BASE_URL)
    dataset = extract_data(fund_url(BASE_URL, cookie_string), cookie_string)

    print('Saving Output...')
    pd.DataFrame(np.array(dataset)[:, :-1].tolist(), columns=['date', 'fund_name', 'nav/unit', 'selling_price', 'redemption_price', 'unknow1', 'nav', 'unknow2'])\
        .to_csv('out/result.csv')
    print('Success!')

main()
upload_file_functional()
stop_Ec2()