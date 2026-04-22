import os, sys, pika, time, random
from datetime import datetime, timedelta
from pandas import DataFrame, read_sql
from argparse import ArgumentParser
from threading import Thread
from numpy import mean, std
import sqlalchemy as sql
from uuid import uuid4
from msgpack import packb, unpackb
import msgpack

DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "1234")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SIDES = [" buy", "sell"]  ;  BASES = ["EUR", "GBP", "USD", "AUD"]

def fake_message():
    return msgpack.packb((                        #  8 bytes (list) => 8 = 7 elements + 1 (separator? checksum?)
        int(time.time() * 1e6),                   #  8 bytes (long int) reception timestamp
        str(uuid4()).split("-")[-1],              # 12 bytes (string) hex ID, 12 chars
        "EURUSD",                                 #  6 bytes (string) symbol, 6 chars
        random.random() > 0.5,                    #  1 byte  (bool) buy = true, sell = false
        1.2 + round(random.random() / 10, 5),     #  8 bytes (double) entry price
        1.1 + round(random.random() / 10, 5),     #  8 bytes (double) stop loss price
        1.3 + round(random.random() / 10, 5),     #  8 bytes (double) take profit price
    ))                                            #  => sum = 58 bytes

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Base(Thread):

    def __init__(self, lineclear = False):
        params = pika.ConnectionParameters("localhost")
        self.conn = pika.BlockingConnection(params)
        self.channel = self.conn.channel()
        self.channel.queue_declare("orders")
        self.endp = "" if lineclear else "\n"
        self.active = False
        super().__init__()

    def close(self):
        self.conn.close()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Producer(Base):

    def __init__(self, lineclear: bool = False, freq: float = 0.05):
        super().__init__(lineclear)
        self.freq, self.n = freq, 1

    def run(self): self.produce()

    def produce(self):
        self.active = True
        while self.active:
            self.main()

    def main(self):
        try:
            message = fake_message()
            print("\rMessage outgoing ->", end = self.endp)
            self.channel.basic_publish(exchange = "", routing_key = "orders", body = message)
            time.sleep(self.freq)
            self.n += 1
        except KeyboardInterrupt:
            self.active = False

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Consumer(Base):

    def __init__(self, lineclear: bool = False, window: int = 1000):
        super().__init__(lineclear)
        self.mean = None
        self.hist = dict()
        self.window = window
        self.db = sql.create_engine(DB_URL)
        self.started = int(time.time() * 1e6)
        self.ax = None
        self.n = 1


    def consume(self):
        self.active = True
        self.channel.basic_consume(queue = "orders",
            on_message_callback = self.on_message, auto_ack = True)
        self.channel.start_consuming()

    def run(self): self.consume()

    def on_message(self, channel, method, properties, message: bytes):
        try:
            message: list = unpackb(message)
            now = int(time.time() * 1e9)
            ts = message.pop(0) * 1000
            delay = now - int(ts)
            if (delay > 1e8): return
            if (self.mean is None): self.mean = delay
            self.mean = self.mean + (delay - self.mean) / self.window
            self.hist[ts] = (delay, self.mean, "rabbitmq-ser", 0)
            print("\r(%d) Message incoming. Mean: %d ns" % (self.n, self.mean), end = self.endp)
            self.n += 1

            if (self.n % self.window == 0):
                for ts, (delay, mean, medium, stdv) in self.hist.items():
                    Q = "INSERT INTO tc_msgbktest VALUES (%d, %d, %d, '%s', %d);"
                    Q = Q % (ts / 1e3, delay, mean, medium, stdv)
                    self.db.execute(Q)

        except KeyboardInterrupt:
            self.active = False
            self.channel.stop_consuming()
            self.channel.close()
            self.join()

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    a: bytes = fake_message()
    print("new message:", a, len(a))
    print("decoded:", unpackb(a), len(a))


    if False:
        parser = ArgumentParser()
        now = int(time.time() * 1e6)
        parser.add_argument("-c", "--consumer", default = False, action = "store_true")
        parser.add_argument("-l", "--lineclear", default = True, action = "store_true")
        parser.add_argument("-s", "--single_th", default = True, action = "store_true")
        parser.add_argument("-p", "--plot_stats", default = False, action = "store_true")
        parser.add_argument("-f", "--freq", default = 0.05, type = float)
        args = parser.parse_args()
        C = getattr(args, "consumer", False)
        L = getattr(args, "lineclear", False)
        P = getattr(args, "plot_stats", False)
        freq = getattr(args, "freq", 0.01)

        if not P:
            if getattr(args, "single_th", False):
                producer = Producer(lineclear = L, freq = freq)
                consumer = Consumer(lineclear = L)
                consumer.start()
                while True:
                    producer.produce()
            else:
                if C: #Consumer(lineclear = L).start()
                    consumer = Consumer(lineclear = L)
                    while True:
                        msgs = consumer.channel.consume("orders", auto_ack = True)
                        for method, properties, message in list(msgs):
                            consumer.on_message(message)
                else: 
                    producer = Producer(lineclear = L, freq = freq)
                    while True: producer.produce()

        else:
            conn = sql.create_engine(DB_URL)
            started = int((time.time() - 10800) * 1e6)
            Q = f"SELECT * FROM tc_msgbktest WHERE (medium = 'rabbitmq-hf');"
            df = read_sql(sql = Q, con = conn, index_col = "timestamp"); dmean = df["delay_ns"].mean()
            df = df.loc[df["delay_ns"] < dmean * 3]; dmean2 = df["delay_ns"].mean()
            ax = df["delay_ns"].hist(bins = 100, figsize = (16, 6), color = "hotpink", alpha = 0.8)
            title = "Message Latency Histogram (RabbitMQ, single-threaded, sample = %d)" % len(df)
            title += "\n" + "‾" * int(len(title) * 1.2)
            ax.set_title(title, fontsize = 14, fontweight = "bold")
            ax.set_xlabel("Latency (ms)", fontsize = 12, fontweight = "bold")
            ax.set_ylabel("Count", fontsize = 12, fontweight = "bold")
            ax.axvline(dmean, color = "red", lw = 2, ls = "--")
            ax.axvline(dmean2, color = "black", lw = 2, ls = ":")
            ax.tick_params(labelsize = 12, labelcolor = "black")
            ax.tick_params(axis = "x", rotation = 90)
            xmax = int(dmean * 3 + 1)
            xticks = range(0, xmax, 200000)
            xlabels = ["%.1f" % (x / 1e6) for x in xticks]
            ax.set_xticks(xticks)
            ax.set_xticklabels(xlabels)
            ax.set_yticks(range(0, len(df) // 3, 200))
            ax.set_xlim(xmin = 0, xmax = dmean * 3)
            ax.figure.set_tight_layout(True)
            ax.figure.savefig("./test-msg-broker.jpg")
            ax.figure.clear()
