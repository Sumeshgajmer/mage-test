if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from kafka import KafkaProducer
from random import random
import json
import time


topic = 'topic-name'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
)

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    publish_messages(data)
    producer.flush()
    # Specify your data exporting logic here

def publish_messages(data):
    for index, row in data.iterrows():
        print(row.to_json().encode('utf-8'))
        producer.send(topic, row.to_json().encode('utf-8'))