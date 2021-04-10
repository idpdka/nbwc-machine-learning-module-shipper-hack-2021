import boto3
import json
import os

# Init SQS and S3 client
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get data from bucket
    raw_data = s3.get_object(Bucket='nbwc-ml-bucket', Key='test-case/testing_LBQ.json')
    inbound_data = json.loads(raw_data['Body'].read().decode('utf-8'))
    
    # Iterate and send the data one by one into the SQS
    for item in inbound_data:
        # The data is separated into two, actual (to be added into backend) and inference (to be ingested into the ML model)
        payload = {
            "actual_data": item,
            "inference_data": f"{item['received_qty']},{item['damage_qty']},{item['vulnerabilitylevel']},{item['volumecbm']},{item['weightkg']},{item['Durationtilexpired']}"
        }

        response = sqs.send_message(
            QueueUrl=os.environ['SQS_QUEUE_URL'],
            MessageBody=json.dumps(payload)
        )
    
    # Finalize the lambda function
    return {
        'statusCode': 200,
        'body': json.dumps('All data has been sent!')
    }
