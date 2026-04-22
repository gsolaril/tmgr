import os, sys, time, random
from datetime import datetime, timedelta
from pandas import DataFrame, read_sql
from argparse import ArgumentParser
from threading import Thread
from numpy import mean, std
import sqlalchemy as sql
from uuid import uuid4
from msgpack import packb, unpackb
from queue import Queue

from matplotlib.pyplot import subplots, axes

QUEUE = Queue()

DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "1234")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SIDES = [" buy", "sell"]  ;  BASES = ["EUR", "GBP", "USD", "AUD"]

def fake_message():
    return packb((                                #  8 bytes (list) => 8 = 7 elements + 1 (separator? checksum?)
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
        
        self.endp = "" if lineclear else "\n"
        self.active = False
        super().__init__()

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
            #print("\rMessage outgoing ->", message, end = self.endp)
            QUEUE.put(message)
            time.sleep(self.freq)
            self.n += 1
        except KeyboardInterrupt:
            self.active = False
            self.join()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Consumer(Base):

    def __init__(self, lineclear: bool = False, window: int = 1000, freq: float = 0.05):
        super().__init__(lineclear)
        self.mean = None
        self.freq = freq
        self.hist = dict()
        self.window = window
        self.db = sql.create_engine(DB_URL)
        self.started = int(time.time() * 1e6)
        self.ax = None
        self.n = 1

    def run(self):
        while True:
            self.consume()

    def consume(self):
        try:
            message = QUEUE.get()
            message: list = unpackb(message)
            now = int(time.time() * 1e9)
            ts = message.pop(0) * 1000
            delay = now - int(ts)
            if (delay > 1e8): return
            if (self.mean is None): self.mean = delay
            self.mean = self.mean + (delay - self.mean) / self.window
            self.hist[ts] = (delay, self.mean, "queue", 0)
            print("\r(%d) Message incoming. Mean: %d ns => %s" % (self.n, self.mean, message), end = self.endp)
            self.n += 1

            if (self.n % self.window == 0):
                for ts, (delay, mean, medium, stdv) in self.hist.items():
                    Q = "INSERT INTO tc_msgbktest "
                    Q += "VALUES (%d, %d, '%s', %d)"
                    Q += " ON CONFLICT DO NOTHING;"
                    Q = Q % (ts / 1e3, delay, medium, 1 / self.freq)
                    self.db.execute(Q)

        except KeyboardInterrupt:
            self.active = False
            self.join()

#█████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):
    
    if True:
        parser = ArgumentParser()
        now = int(time.time() * 1e6)
        parser.add_argument("-c", "--consumer", default = False, action = "store_true")
        parser.add_argument("-l", "--lineclear", default = True, action = "store_true")
        parser.add_argument("-s", "--single_th", default = False, action = "store_true")
        parser.add_argument("-p", "--plot_stats", default = True, action = "store_true")
        parser.add_argument("-f", "--freq", default = 0.0002, type = float)
        args = parser.parse_args()
        C = getattr(args, "consumer", False)
        L = getattr(args, "lineclear", False)
        P = getattr(args, "plot_stats", False)
        mps = getattr(args, "freq", 0.005)

        if not P:
            if getattr(args, "single_th", False):
                producer = Producer(lineclear = L, freq = mps)
                consumer = Consumer(lineclear = L, freq = mps)
                consumer.start()
                while True:
                    producer.produce()
            else:
                if C: #Consumer(lineclear = L).start()
                    consumer = Consumer(lineclear = L, freq = mps)
                    while True: consumer.consume()
                else: 
                    producer = Producer(lineclear = L, freq = mps)
                    while True: producer.produce()

        else:
            conn = sql.create_engine(DB_URL)
            started = int((time.time() - 10800) * 1e6)
            print("DOING QUERY")
            Q = f"SELECT * FROM tc_msgbktest WHERE (medium = 'zmq');"
            df = read_sql(sql = Q, con = conn, index_col = "timestamp");
            df["delay_ns"] = (df["delay_ns"] / 1e3).astype(int)
            mps = sorted(df["mps"].unique())

            df: DataFrame = df.reset_index().set_index(["mps", "timestamp"]).sort_index()
            fig, axs = subplots(len(mps), 1, figsize = (16, len(mps) * 2), sharex = True)
            n_axs = len(axs := dict(zip(mps, axs)))
            title = "Message Latency Histograms (ZMQ, single-threaded)"
            title += "\n" + "‾" * int(len(title) * 1.2) + "\n"
            fig.suptitle(title, fontsize = 18, fontweight = "bold")
            max_mean = 0
            print("DONE QUERY")

            for n, (mps, ax) in enumerate(axs.items(), 1):

                df_f: DataFrame = df.loc[mps].copy()
                dmean = df_f["delay_ns"].mean()
                df_f = df_f.loc[df_f["delay_ns"] < dmean]
                dmean2 = df_f["delay_ns"].mean()
                max_mean = max(dmean, max_mean)
                color = (n / n_axs, 0, 1 - n / n_axs)
                ax.axvline(dmean2, color = "red", lw = 2, ls = "--")
                df_f["delay_ns"].hist(ax = ax, bins = 100, color = color, alpha = 0.75)
                ax.set_ylabel("Count" % mps, fontsize = 14, fontweight = "bold")
                text = "mean = %.1f, msg/sec = %d" % (dmean2, mps)
                text += "\ndelay/interval ratio = %.2f%%" % ((dmean2 * mps * 1e-6) * 1e2)
                text += "\nsample = %d" % len(df_f)
                ax.text(x = 0.98, y = 0.95, s = text, ha = "right", va = "top",
                    transform = ax.transAxes, fontsize = 14, fontweight = "bold")
                
                print("\rDone plot #%d/%d: %d msg/sec" % (n, n_axs, mps), end = "")
            
            ax.set_xlabel("Latency (us)", fontsize = 12, fontweight = "bold")
            ax.tick_params(labelsize = 12, labelcolor = "black")
            ax.tick_params(axis = "x", rotation = 90)
            ax.set_xticks(range(0, int(max_mean + 1), 50))
            ax.set_xlim(1, max_mean)
            print("DONE")
            fig.set_tight_layout(True)
            fig.savefig("./test-msg-zeromq.jpg")
            fig.clear()
