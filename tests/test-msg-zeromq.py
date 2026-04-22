import os, sys, zmq, time, random
from datetime import datetime, timedelta
from pandas import DataFrame, read_sql
from argparse import ArgumentParser
from msgpack import packb, unpackb
from loguru import logger as Log
from threading import Thread
import sqlalchemy as sql
from uuid import uuid4

DEFAULT_MPS = 50000
CONTEXT = zmq.Context()
URL_TCP = "tcp://127.0.0.1:5555"

DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "1234")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

def fake_message(n: int = 0):
    return packb((n,                            #  8 bytes (list) => 8 = 7 elements + 1 (separator? checksum?)
        int(time.time() * 1e6),                 #  8 bytes (long int) reception timestamp
        str(uuid4()).split("-")[0],             #  8 bytes (string) hex ID, 8 chars
        "EURUSD",                               #  6 bytes (string) symbol, 6 chars
        random.random() > 0.5,                  #  1 byte  (bool) buy = true, sell = false
        round(1.2 + random.random() / 10, 5),   #  8 bytes (double) entry price
        round(1.1 + random.random() / 10, 5),   #  8 bytes (double) stop loss price
        round(1.3 + random.random() / 10, 5),   #  8 bytes (double) take profit price
    ))                            

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Base(Thread):

    def __init__(self, lineclear = False):
        self.endp = lineclear * "\n"
        self.active = False
        super().__init__()

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

class Producer(Base):

    def __init__(self, lineclear: bool = False, mps: int = DEFAULT_MPS):

        super().__init__(lineclear)
        print("mps = %d" % mps)
        self.mps, self.n = mps, 1
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(URL_TCP)
        #self.socket.setsockopt(zmq.LINGER, 0)
        self.n = 1

    def close(self):
        self.socket.close()
        self.context.term()

    def run(self):
        self.active = True
        while self.active:
            self.produce()

    def produce(self):
        try:
            message = fake_message(self.n)
            self.socket.send(message)
            time.sleep(1 / self.mps)
            self.n = self.n + 1
        except KeyboardInterrupt:
            self.active = False
            self.close()

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████
            
class Consumer(Base):

    def __init__(self, lineclear: bool = False, mps: int = DEFAULT_MPS, window: int = 1000):

        super().__init__(lineclear)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(URL_TCP)
        self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        #self.socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.window, self.mean = window, None
        self.db = sql.create_engine(DB_URL)
        self.hist = dict()
        self.mps = mps
        self.last_n = 0

    def close(self):
        self.socket.close()
        self.context.term()

    def run(self):
        self.active = True
        while self.active:
            self.consume()

    def consume(self):
        try:
            message: list = unpackb(self.socket.recv())
            timestamp = int(time.time() * 1e9)
            trans = (n_new := message.pop(0)) - self.last_n
            if (trans != 1): print("QUEUE INVERTED: %d -> %d" % (self.last_n, n_new))
            delay = timestamp - message.pop(0) * 1e3
            if (self.mean is None): self.mean = delay
            self.mean = self.mean + (delay - self.mean) / self.window
            self.hist[timestamp] = (delay, self.mean, "zmq", 0)
            verbose = "\rReceived #%d (trans: %d, delay: %d, mean: %d) =>"
            print(verbose % (n_new, trans, delay, self.mean), message, end = "")

            self.last_n = n_new
            
        except KeyboardInterrupt:
            self.active = False
            self.close(), print()
            hsize = len(self.hist)
            for n, (timestamp, (delay, mean, medium, stdv)) in enumerate(self.hist.items(), 1):

                print("\rUploading row %d/%d" % (n, hsize), end = "")
                Q = "INSERT INTO tc_test_msgbk VALUES (%d, %d, '%s', %d)"
                Q = Q % (timestamp / 1e3, delay, medium, self.mps)
                self.db.execute(Q + " ON CONFLICT DO NOTHING;")

            print("\n%d mps, Finished uploading %d rows." % (self.mps, len(self.hist)))

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████
            
if (__name__ == "__main__"):

    parser = ArgumentParser()
    parser.add_argument("-c", "--consumer", action = "store_true", default = False)
    parser.add_argument("-l", "--lineclear", action = "store_true", default = False)
    parser.add_argument("-m", "--mps", type = int, default = DEFAULT_MPS)
    args = parser.parse_args()
    C = getattr(args, "consumer", False)
    L = getattr(args, "lineclear", False)
    M = getattr(args, "mps", DEFAULT_MPS)
    
    if C: #Consumer(lineclear = L).start()
        print("Using consumer...")
        consumer = Consumer(lineclear = L, mps = M)
        consumer.run()
    else:
        print("Using producer...")
        producer = Producer(lineclear = L, mps = M)
        producer.run()