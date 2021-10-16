import os
import json
from kafka import KafkaProducer
import boto3

from transform import raw_table_cust_transform, raw_table_acct_transform

# Simple 2 node MSK
MSK_CLUSTER = [os.environ['MSK1'], os.environ['MSK2']]
FIREHOSE_STREAM = os.environ['FIREHOSE_STREAM']

def lambda_handler(event, context):
    payload = event

    firehose = boto3.client('firehose')
    response = firehose.put_record(
        DeliveryStreamName=FIREHOSE_STREAM,
        Record={'Data': bytes(payload, 'utf-8')}
    )

    producer = KafkaProducer(
        bootstrap_servers=MSK_CLUSTER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )    

    if payload['topic'] == 'raw_table_cust':
        transformed = raw_table_cust_transform(payload)
        producer.send('customer', value=transformed['customer'])
        producer.send('individual', value=transformed['individual'])
    elif payload['topic'] == 'raw_table_acct':
        transformed = raw_table_acct_transform(payload)
        producer.send('account', value=transformed)

    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }
