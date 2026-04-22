import os, requests, time
import sqlalchemy as sql
import asyncio, websockets
from pandas import DataFrame, read_sql
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

trigger = IntervalTrigger(seconds = 2)

API_URL = "{protocol}://mt5.mtapi.be/"
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "1234")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#======================================================================================

def parallel_task():
    print(datetime.utcnow())

scheduler = BackgroundScheduler()
scheduler.add_job(func = parallel_task,
    trigger = trigger, name = "cron_task")

scheduler.start()

#======================================================================================

engine = sql.create_engine(DB_URL)

accounts = ["coco_test_oanda_5", "coco_test_icmarkets_5"]
query = str.join(", ", ["'%s'" % acc for acc in accounts])
query = "SELECT * FROM tc_accounts WHERE alias IN (%s)" % query
accounts = read_sql(sql = query, con = engine, index_col = "alias")
accounts["host"] = accounts["ip"].str.split(":").str[0]
accounts["port"] = accounts["ip"].str.split(":").str[1]
accounts = accounts.rename(columns = {"id": "user"})

#======================================================================================

for alias, row in accounts.iterrows():
    url = API_URL.format(protocol = "https") + "Connect"
    token = requests.get(url, params = {**row})
    accounts.at[alias, "token"] = token.text


accounts["subs_url"] = API_URL.format(protocol = "https") + "SubscribeOrderUpdate?id=" + accounts["token"]

accounts["connected"] = accounts["subs_url"].map(requests.get)

accounts["ws_url"] = API_URL.format(protocol = "wss") + "Events?id=" + accounts["token"]

websocket_urls = accounts["ws_url"].tolist()
print("websocket_urls:")
for alias, row in accounts.iterrows():
    verbose = ": {subs_url} - {ws_url}"
    print(alias + verbose.format(**row))

#======================================================================================

async def connect_to_websocket(url):

    t_start = datetime.utcnow()
    duration = timedelta(seconds = 30)
    print("Test started at %s" % t_start.strftime("%X"))
    async with websockets.connect(url) as websocket:

        while (datetime.utcnow() - t_start < duration):
            # Your WebSocket handling logic goes here
            message = await websocket.recv()
            print(f"Received message from {url}: {message}")

        t_finish = datetime.utcnow()
        print("Test finished at %s" % t_finish.strftime("%X"))

#======================================================================================

async def main():

    # Create tasks to connect to each WebSocket
    tasks = [asyncio.create_task(connect_to_websocket(url)) for url in websocket_urls[:1]]

    # Wait for all tasks to complete
    await asyncio.gather(*tasks)

    tasks = [asyncio.create_task(connect_to_websocket(url)) for url in websocket_urls[1:]]

    await asyncio.gather(*tasks)

#======================================================================================
#======================================================================================
    
if (__name__ == "__main__"):
    try: asyncio.run(main())
    except KeyboardInterrupt:
        print(KeyboardInterrupt)