# nymex_async_wrapper.py
import asyncio
import numpy as np
from ib_insync import IB, Contract, MarketOrder
from typing import Tuple
from config import NYMEX_API_KEY, NYMEX_HOST, NYMEX_PORT, NYMEX_CONTRACTS
from datetime import datetime

class NymexAsyncWrapper:
    def __init__(self, api_key, host, port, contracts):
        self.api_key = api_key
        self.host = host
        self.port = port
        self.contracts = contracts
        self.active_tickers = {} # dict of active trades (localSymbol : Ticker)
        self.order_filled_events = {}

        # code for self.symbols to load localSymbols for each contract in the similar list  fashion as self.contracts
        self.symbols = [contract.localSymbol for contract in self.contracts]
        print(f"{datetime.now()}: nymex local symbols: {self.symbols}")
        
        self.positions = {}
        for contract in self.contracts:
            self.positions[contract.localSymbol] = 0
        self.current_bid = {}
        self.current_ask = {}
        for contract in self.contracts:
            self.current_bid[contract.localSymbol] = np.nan
            self.current_ask[contract.localSymbol] = np.nan

        self.order_time = {}
        self.submit_time = {}
        self.fill_time = {}


        self.ib = IB()

    async def initialize(self):
        await self.ib.connectAsync(self.host, self.port, clientId=self.api_key)
        self.contracts = await self.ib.qualifyContractsAsync(*self.contracts)
        self.load_positions(self.symbols)
        print(self.positions)
        self.ib.orderStatusEvent += self.on_order_status # move from send_order definition..

    def on_pending_tickers(self, tickers):
        for ticker in tickers:
            self.current_bid[ticker.contract.localSymbol] = ticker.bid
            self.current_ask[ticker.contract.localSymbol] = ticker.ask


    async def get_bid_ask(self, contract):
        # Request current market data
        ticker_snapshot = self.ib.reqMktData(contract, '', snapshot = True, regulatorySnapshot = False)
        # waiting until the data is there TODO: STOPPED HERE, LOOP NEVER ENDS
        while np.isnan(ticker_snapshot.bid) or np.isnan(ticker_snapshot.ask):
            print(ticker_snapshot.bid)
            print(ticker_snapshot.ask)
            print(f"{datetime.now()}: ib_insync np.isnan loop")
            await asyncio.sleep(0.1)  # sleep for 100 ms
        return ticker_snapshot.bid, ticker_snapshot.ask



# TODO: check if contract definition logic is accurate
    async def subscribe_bid_ask(self, contract):
        # Request current market data
        ticker_snapshot = self.ib.reqMktData(contract, '', snapshot = True, regulatorySnapshot = False)
        # waiting until the data is there TODO: STOPPED HERE, LOOP NEVER ENDS
        while np.isnan(ticker_snapshot.bid) or np.isnan(ticker_snapshot.ask):
            print(ticker_snapshot.bid)
            print(ticker_snapshot.ask)
            print(f"{datetime.now()}: np.isnan loop")
            await asyncio.sleep(0.1)  # sleep for 100 ms
        self.current_bid[contract.localSymbol] = ticker_snapshot.bid
        self.current_ask[contract.localSymbol] = ticker_snapshot.ask
        # subscribe to tick data updates
        ticker = self.ib.reqTickByTickData(contract, tickType='BidAsk',ignoreSize = True)
        self.active_tickers[contract.localSymbol] = ticker


    async def unsubscribe_bid_ask(self, contract):
        if contract.localSymbol in self.active_tickers:
            ticker = self.active_tickers[contract.localSymbol]
            self.ib.cancelTickByTickData(ticker.contract, tickType='BidAsk')
            del self.active_tickers[contract.localSymbol]


    async def send_order(self, contract, action, volume):
        order = MarketOrder(action, volume)
        trade = self.ib.placeOrder(contract, order)
        self.order_time[trade.order.orderId] = datetime.now()
        self.order_filled_events[trade.order.orderId] = asyncio.Event()
        
        return trade.order.orderId

# callback function that is called whenever the status of any order changes
    def on_order_status(self, trade):
        if trade.orderStatus.status == 'Submitted':
            self.submit_time[trade.order.orderId] = datetime.now()
            print(f"{datetime.now()}: Order {trade.order.orderId}: submit latency = {self.submit_time[trade.order.orderId] - self.order_time[trade.order.orderId]}")
        elif trade.orderStatus.status == 'Filled':
            self.fill_time[trade.order.orderId] = datetime.now()
            print(f"{datetime.now()}: Order {trade.order.orderId}: fill latency = {self.fill_time[trade.order.orderId] - self.order_time[trade.order.orderId]}")
            if trade.order.orderId in self.order_filled_events:
                self.order_filled_events[trade.order.orderId].set()


# To rewrite:
    async def modify_order(self, order, new_volume=None, new_price=None):
        if new_volume is not None:
            order.order.totalQuantity = new_volume
        if new_price is not None:
            order.order.lmtPrice = new_price
        await self.ib.placeOrderAsync(order.contract, order.order)

# To rewrite:
    async def cancel_order(self, order):
        await self.ib.cancelOrderAsync(order)

# INCORRECT NEED TO REWRITE
    async def check_margin_requirements(self, contract, order_type, volume):
        contract = Contract(symbol=contract, secType="FUT", exchange="NYMEX")
        order = MarketOrder(order_type, volume)
        margin = await self.ib.whatIfOrderAsync(contract, order)
        return float(margin.initMarginChange), float(margin.maintMarginChange)


    def load_positions(self,symbols):
        
        # Initialize self.positions[symbol] to 0 for all symbols
        for symbol in symbols:
            self.positions[symbol] = 0

        # Request all open positions
        ib_positions = self.ib.positions()

        # Filter the positions for the symbols of interest
        for position in ib_positions:
            symbol = position.contract.localSymbol
            if symbol in symbols:
                # Use localSymbol as a unique key
                self.positions[symbol] = position.position


    async def run(self):
        for contract in self.contracts:
            await self.subscribe_bid_ask(contract)

        self.ib.pendingTickersEvent  += self.on_pending_tickers

        while True:
            await asyncio.sleep(1)

    def __del__(self):
        self.ib.disconnect()