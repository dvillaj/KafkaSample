# encoding: utf-8

import threading, logging, time
import multiprocessing
import argparse
import json
import sys

from kafka import KafkaConsumer, KafkaProducer, TopicPartition


class Consumer(multiprocessing.Process):
    def __init__(self, args):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.topic = args.topic
        self.groupid = args.groupid
        self.server = "%s:%s" % (args.server, args.port)
        self.partition = args.partition
        self.inicio = args.inicio
        self.offset = args.offset
        self.words = args.words
      
    def stop(self):
        self.stop_event.set()
        
    def run(self):

        consumer = KafkaConsumer(
            bootstrap_servers=self.server, 
            auto_offset_reset='earliest',
            group_id=self.groupid)

        if consumer.partitions_for_topic(self.topic) is None:
            print("El tópico %s no existe!" % self.topic)
            sys.exit(2)

        if self.partition is None:
            partitions = [TopicPartition(self.topic, partition) 
                           for partition in consumer.partitions_for_topic(self.topic)]
        else:
            partitions = [TopicPartition(self.topic, int(self.partition))]

        consumer.assign(partitions)

        if self.offset is None:
            if self.inicio:
                for partition in partitions:
                    consumer.seek_to_beginning(partition)
        else:
            for partition in partitions:
                    consumer.seek(partition, int(self.offset))

        while not self.stop_event.is_set():
            try:
                for message in consumer:
                    logging.info(message)

                    try:
                        valor = json.loads(message.value)
                        if self.words:
                            valor = valor['words']
                            
                    except (ValueError):
                        valor = message.value.decode('utf-8')

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
parser.add_argument('--server', help="Servidor de kafka", default = "localhost")
parser.add_argument('--port', help="Puerto", default = "9092")
parser.add_argument('--partition', help="Número de particion")
parser.add_argument('--offset', help="Establece el offset")
parser.add_argument("--inicio", help="Recupera los mensajes desde el inicio",
                 action='store_true', default = False)
parser.add_argument("--words", help="Muestra las palabras del mensaje",
                 action='store_true', default = False)

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
    try:
        main()
    except:
        pass
