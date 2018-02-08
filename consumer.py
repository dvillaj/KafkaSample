# encoding: utf-8

import threading, logging, time
import multiprocessing
import argparse
import json

from kazoo.client import KazooClient
from kafka import KafkaConsumer, KafkaProducer, TopicPartition, __version__


class Consumer(multiprocessing.Process):
    def __init__(self, args):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.topic = args.topic
        self.groupid = args.groupid
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):

        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 #auto_offset_reset='earliest',
                                 #auto_offset_reset='largest',
                                 group_id= self.groupid,
                                 #enable_auto_commit = True,
                                 )

        #partition = TopicPartition(self.topic, 0)
        consumer.subscribe([self.topic])
        #consumer.assign([partition])
        #consumer.seek(partition, 10)


        #print(consumer.assigned_partitions())
        #consumer.seek(partition = TopicPartition(self.topic, 0), offset = 2)

        while not self.stop_event.is_set():
            try:
                for message in consumer:
                    logging.info(message)

                    try:
                        valor = json.loads(message.value)['words']
                    except (ValueError):
                        valor = message.value

                    print ("Recibiendo Mensaje (%s/%d/%d) %s" % (message.topic, 
                                              message.partition,
                                              message.offset, 
                                              #message.key,
                                              valor))

                    if self.stop_event.is_set():
                        break
            except IndexError:
                pass

        consumer.close()
        


parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
parser.add_argument('--groupid', help="Grupo de consumidor")

args = parser.parse_args()

if args.topic is None:
    parser.error("Es necesario especificar un tópico kafka!")
    sys.exit(1)



def main():
    tasks = [
        Consumer(args)
    ]

    for t in tasks:
        t.start()
    
    print("Presiona Control+C para parar ...")
    try:
        while True: 
            time.sleep(0.1)

    except (KeyboardInterrupt, SystemExit):
        pass
    
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
        
        
if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARN
        )

    logging.warn("Verión %s" % __version__)
    try:
        main()
    except:
        pass
