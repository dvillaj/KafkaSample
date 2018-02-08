# encoding: utf-8

from kafka import SimpleClient
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")

args = parser.parse_args()

brokers = 'localhost:9092'
topic = args.topic

client = SimpleClient(brokers)

partitions = client.topic_partitions[topic]
offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]
offsets_responses = client.send_offset_request(offset_requests)

for r in offsets_responses:
    print "partition = %s, offset = %s"%(r.partition, r.offsets[0])