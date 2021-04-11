import os
import io
import boto3
import json
import csv
import pymysql
import requests
import datetime
import

# grab environment variables
SAGEMAKER_ENDPOINT_NAME = os.environ['SAGEMAKER_ENDPOINT_NAME']
BACKEND_API_ENDPOINT = os.environ['BACKEND_API_ENDPOINT']
runtime = boto3.client('runtime.sagemaker')

try:
    conn = pymysql.connect(host=os.environ['MYSQL_HOST'],user=os.environ['MYSQL_USERNAME'],
                           passwd=os.environ['MYSQL_PASSWORD'],db=os.environ['MYSQL_DATABASE'],
                           connect_timeout=5,
                           cursorclass=pymysql.cursors.DictCursor)
except:
    sys.exit()

def convert_date_to_unix_timestamp(date, form):
    return int(time.mktime(datetime.datetime.strptime(string, form).timetuple()))
    
def lambda_handler(event, context):
    for record in event['Records']:
        data = json.loads(record["body"])
        inference_payload = data["inference_data"]
        actual_payload = data["actual_data"]
        inference_result = None
        category_number = None
        
        try:
            response = runtime.invoke_endpoint(EndpointName=SAGEMAKER_ENDPOINT_NAME,
                                          ContentType='text/csv',
                                          Body=inference_payload)
            inference_result = response['Body'].read().decode().split(',')
            print(f"Predicted Label: {inference_result[0]}, Probability: {inference_result[1]}")
            category_number = inference_result[0]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error on inferencing: {e}')
            }
        
        try:
            response = requests.get(url=f"{BACKEND_API_ENDPOINT}/api/v1/inbound/count-location-bins")
            available_location_bins_data = response.json()
            category_id = None
            location_bin_data = None
            assigned_location_bin = None
            assigned_location_bin_id = None
            
            for item in available_location_bins_data:
                if item["category"]["category_number"] == int(category_number):
                    location_bin_data = item["location_bin_counts"]
                    category_id = item["category"]["id"]
                    break
            
            for loc_bin in location_bin_data:
                if loc_bin["count"] < loc_bin["location_bin"]["capacity"]:
                    assigned_location_bin = loc_bin["location_bin"]["name"]
                    assigned_location_bin_id = loc_bin["location_bin"]["id"]
                    break
                
            actual_payload["is_automated"] = False
            actual_payload["is_dispatch"] = False
            actual_payload["is_done"] = False
                
            if assigned_location_bin != None:
                actual_payload["category_id"] = category_id
                actual_payload["location_bin_id"] = assigned_location_bin_id
                actual_payload["is_automated"] = True
                actual_payload["is_automated"] = True
                actual_payload["is_dispatch"] = True

            response = requests.post(url=f"{BACKEND_API_ENDPOINT}/api/v1/inbound/count-location-bins", data=actual_payload)
            
            with conn.cursor() as cur:
                qry = "INSERT INTO `put_away_lists` (`po_no`, `location_bin`) VALUES (%s, %s)"
                cur.execute(qry, (actual_payload["po_no"], assigned_location_bin))

            conn.commit()
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error on assigning location bin: {e}')
            }

        return {
            'statusCode': 200,
            'body': json.dumps('Data has been assigned and recorded!')
        }