# encoding: utf-8

from kafka import SimpleClient
from kafka.errors import KafkaUnavailableError, NoBrokersAvailable
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
parser.add_argument('--brokers', help="Broker en el formato dirección:puerto", default = "localhost:9092")

args = parser.parse_args()

brokers = args.brokers
topic = args.topic

try:
    client = SimpleClient(brokers)
except KafkaUnavailableError:
    print("El servidor de Kafka no se encuentra disponible!")
    sys.exit(0)


partitions = client.topic_partitions[topic]
offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]
offsets_responses = client.send_offset_request(offset_requests)

for r in offsets_responses:
    print "partition = %s, offset = %s"%(r.partition, r.offsets[0])