# encoding: utf-8

import threading, time
import multiprocessing, logging
from random_words import RandomWords
import argparse
import json
import random
import uuid
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

rw = RandomWords()

class Producer(threading.Thread):
    def __init__(self, args):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.topic = args.topic
        self.time = float(args.time)
        self.words = int(args.words)
        self.json = args.json
        self.server = "%s:%s" % (args.server, args.port)
        self.nombre = args.name
        self.letter = args.letter
        
    def stop(self):
        self.stop_event.set()


    def run(self):
        producer = KafkaProducer(bootstrap_servers=self.server)

        n = 1

        while not self.stop_event.is_set():
            words = rw.random_words(letter = self.letter, count=self.words)
            message = ' '.join(words).encode('utf-8')

            if (self.json):
                json_message = { 'n' : n,
                         'words' : message,
                         'number' : random.randrange(1000) , 
                         'timestamp' : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                         'id' : str(uuid.uuid4().hex).encode('utf-8')
                        }

                if self.nombre:
                    json_message['nombre'] = self.nombre

                message = json.dumps(json_message).encode('utf-8')

            future = producer.send( self.topic, message )
            record_metadata = future.get(timeout=10)

            print("Enviando mensaje (%s/%s/%s): %s" % (record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
                message))

            time.sleep(self.time)
            n = n + 1

        producer.close()

        
        
parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
parser.add_argument("--time", help="Tiempo de espera entre mensajes", default = "1")
parser.add_argument("--words", help="Número de palabras", default = "1")
parser.add_argument("--seed", help="Semilla aleatoria")
parser.add_argument("--name", help="Nombre del agente")
parser.add_argument("--letter", help="Letra por la que empiezan las palabras")
parser.add_argument("--json", help="Mensaje en formato json", action='store_true', default = False)
parser.add_argument('--server', help="Servidor de kafka", default = "localhost")
parser.add_argument('--port', help="Puerto", default = "9092")

args = parser.parse_args()

if args.topic is None:
    parser.error("Es necesario especificar un tópico kafka!")
    sys.exit(1)

logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARN
        )

if not args.seed is None and args.seed.isdigit():
    random.seed(int(args.seed))
else:
    random.seed(datetime.now())

if __name__ == "__main__":
    tasks = [
        Producer(args),
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