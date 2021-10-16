import os
import json
from kafka import KafkaConsumer
import boto3

# Simple Kafka on docker compose so same host for all brokers
KAFKA_IP = os.environ.get('KAFKA_IP', 'localhost')

consumer = KafkaConsumer(
    bootstrap_servers=[f'{KAFKA_IP}:9093', f'{KAFKA_IP}:9094', f'{KAFKA_IP}:9095'],
    auto_offset_reset='latest', enable_auto_commit=True,
    auto_commit_interval_ms=1000)

consumer.subscribe(topics=['raw_table_acct','raw_table_cust'])

lambda_client = boto3.client('lambda')

for message in consumer:
    value = json.loads(message.value)
    
    payload = {"topic": message.topic, "partition": message.partition, "offset": message.offset, "key": message.key, "value": value }

    response = lambda_client.invoke(   
        FunctionName='test_kafka_s3',    
        Payload=json.dumps(payload),
    )

    print( response['Payload'].read().decode("utf-8") )
