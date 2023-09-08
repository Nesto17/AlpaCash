from dotenv import load_dotenv
import os
import json
from datetime import datetime
import time
import asyncio
import websockets

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")

hist_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
live_client = StockDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY)

def getStockHistoricalData(client: StockHistoricalDataClient, start_date, end_date):
    bar_request = StockBarsRequest(
        symbol_or_symbols=["GOOGL"],
        timeframe=TimeFrame.Day,
        start=start_date,
        end=end_date
    )
    trade_request = StockTradesRequest(
        symbol_or_symbols=["AAPL"],
        start=start_date,
        end=end_date,
        limit=10
    )

    # res = client.get_stock_bars(bar_request)
    res = client.get_stock_bars(bar_request)
    print(res)

getStockHistoricalData(hist_client, datetime(2023, 8, 26), datetime(2023, 8, 30))

# def getStockDataStream(client: StockDataStream):
#     data_storage = []

#     async def realtime_stock_data_handler(data):
#         data_storage.append(data)
#         print(data)
    
#     client.subscribe_bars(realtime_stock_data_handler, "GOOGL")
#     client.run()
#     time.sleep(60 * 3)
#     client.stop()

# def stopStockDataStream(client: StockDataStream):
#     client.stop()

# # getStockHistoricalData(hist_client, datetime(2023, 8, 10))

# async def handle_stream(uri):
#     async def send_message(websocket, message):
#         message_json = json.dumps(message)
#         await websocket.send(message_json)

#     async with websockets.connect(uri) as websocket:
#         auth_message = {"action": "auth", "key": "AK4ACW7MCLHFQ0EWL4NH", "secret": "OzCD30SMiOvZPQONr9vx35hrw1FfnTKecbH4u3xU"}
#         await send_message(websocket, auth_message)

#         subscription_message = {"action": "subscribe", "bars": ["GOOGL"]}
#         await send_message(websocket, subscription_message)

#         # start_time = asyncio.get_event_loop().time()

#         while True:
#             data = await websocket.recv()
#             print(f"Received: {json.loads(data)}")

#             # if asyncio.get_event_loop().time() - start_time >= 120:
#             #     ubsubscription_message = {"action": "ubsubscribe", "bars": ["*"]}
#             #     await send_message(websocket, ubsubscription_message)
#             #     break

# if __name__ == "__main__":
#     ws_uri = "wss://stream.data.alpaca.markets/v2/iex"

#     asyncio.get_event_loop().run_until_complete(handle_stream(ws_uri))