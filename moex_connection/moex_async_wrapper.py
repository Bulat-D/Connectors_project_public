# moex_async_wrapper.py
import asyncio
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple
from config import MOEX_LOGIN, MOEX_PASSWORD, MOEX_SERVER, MOEX_SYMBOLS, SYMBOL_DECIMALS


class MoexAsyncWrapper:
    def __init__(self, login, password, server, symbols, lot_sizes):

        self.login = login
        self.password = password
        self.server = server
        self.symbols = symbols
        
        self.lots_dict = dict(zip(self.symbols, lot_sizes))
        self.decimals = dict(zip(self.symbols, SYMBOL_DECIMALS))

        self.current_bid = {}
        self.current_ask = {}
        for symbol in self.symbols:
            self.current_bid[symbol] = np.nan
            self.current_ask[symbol] = np.nan

        self.deep_bid = {}
        self.deep_ask = {}
        for symbol in self.symbols:
            self.deep_bid[symbol] = np.nan
            self.deep_ask[symbol] = np.nan

        if not mt5.initialize(): # login=self.login, password=self.password, server=self.server
            raise RuntimeError(f"Error initializing MetaTrader5: {mt5.last_error()}")
        print(mt5.terminal_info())
        print(mt5.version())
        print(mt5.account_info())

    async def initialize(self):
        await self.add_market_book(self.symbols)
        for symbol in self.symbols:
            print(f"{symbol} pose: {self.load_position(symbol)}")

# load access to market book
    async def add_market_book(self, symbols):
        async def add_book(symbol):
            def blocking():
                if not mt5.market_book_add(symbol):
                    print(f"{datetime.now()}: Failed to add market book for {symbol}, error code =", mt5.last_error())
            await asyncio.to_thread(blocking)
        await asyncio.gather(*(add_book(symbol) for symbol in symbols))

# close market book access
    async def release_market_book(self, symbols):
        async def release_book(symbol):
            def blocking():
                if not mt5.market_book_release(symbol):
                    print(f"{datetime.now()}: Failed to release market book for {symbol}, error code =", mt5.last_error())
            await asyncio.to_thread(blocking)
        await asyncio.gather(*(release_book(symbol) for symbol in symbols))


# send market order
    def send_order(self, symbol, order_type, volume):
        
        if order_type == "Buy":
            type = mt5.ORDER_TYPE_BUY
        elif order_type == "Sell":
            type = mt5.ORDER_TYPE_SELL
        else:
            print("{datetime.now()}: incorrect order type")
            return None
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": type,
            #"price": price,
            "sl": 0.0,
            "tp": 0.0,
            #"magic": 123456,
            "deviation": 0,
            "type_time": mt5.ORDER_TIME_DAY,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }

        #loop = asyncio.get_event_loop()
        trade = mt5.order_send(request)
        if trade.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"{datetime.now()}: moex market order sent")
        else:
            print(f"{datetime.now()}: 2. order_send failed, retcode={trade.retcode}")
        print(f"{datetime.now()}: {symbol} error code =", mt5.last_error())
        return trade
        #trade = await loop.run_in_executor(None, mt5.order_send, request) # TESTING
        #return await asyncio.wait_for(trade, timeout=10)

    async def check_order_status(self, ticket):
        checking = True
        while checking:
            await asyncio.sleep(0.1)
            

# send limit order
    def limit_order(self, symbol, order_type, volume, price):

        request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": price,
            "sl": 0.0,
            "tp": 0.0,
            #"magic": 123456,
            "deviation": 0,
            "type_time": mt5.ORDER_TIME_DAY,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }

        #loop = asyncio.get_running_loop()
        print(f"{datetime.now()}: moex limit order: start sending")
        trade = mt5.order_send(request)
        if trade.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"{datetime.now()}: moex limit order: sent successfully")
        else:
            print(f"{datetime.now()}: 2. limit order_send failed, retcode={trade.retcode}")
        print(f"{datetime.now()}: {symbol} error code =", mt5.last_error())
        return trade

# modify open / pending limit order based on order_ticket
    def modify_order(self, order_ticket, price):
        
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": order_ticket,
            "price": price,
            "sl": 0.0,
            "tp": 0.0,
            "type_time": mt5.ORDER_TIME_DAY,
        }

        #loop = asyncio.get_running_loop()
        print(f"{datetime.now()}: moex modify trade: start sending")
        modify_trade = mt5.order_send(request)
        if modify_trade.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"{datetime.now()}: moex modify trade: sent successfully")
        else:
            print(f"{datetime.now()}: 2. modify order_send failed, retcode={modify_trade.retcode}")
        print(f"{datetime.now()}: modify order error code =", mt5.last_error())
        return modify_trade


# cancel open / pending limit order based on order_ticket
    def cancel_order(self, order_ticket):
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": order_ticket,
        }
        #loop = asyncio.get_running_loop()
        print(f"{datetime.now()}: cancel trade - start sending")
        cancel_trade = mt5.order_send(request)
        if cancel_trade.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"{datetime.now()}: cancel trade - sent successfully")
        else:
            print(f"{datetime.now()}: 2. cancel order failed, retcode={cancel_trade.retcode}")
        print(f"{datetime.now()}: error code =", mt5.last_error())
        return cancel_trade
        #loop = asyncio.get_event_loop()
        #return await loop.run_in_executor(None, mt5.order_send, request)

# load open orders for selected moex symbol
    def load_orders(self, symbol):
        return mt5.orders_get(symbol=symbol)

# load open orders for selected tickets
    def load_order_ticket(self, ticket):
        if ticket:
            return mt5.orders_get(ticket=ticket)
        else:
            return None

# load historical orders from today for selected moex symbol
    def load_hist_orders(self, symbol):
        from_date = datetime.now().replace(hour=6, minute=0, second=0, microsecond = 0) # 6:00 is local time, but this get converted to UTC, so it's 2:00 (based on server timezone)
        to_date = datetime.now()+timedelta(days=1) # account for timezone just setting tomorrow's datetime
        return mt5.history_orders_get(from_date, to_date, symbol=symbol)

# load position for selected moex symbol
    def load_position(self, symbol):
        position = mt5.positions_get(symbol = symbol)
        if position is not None:
            if len(position) == 1:
                return ( (position[0].type * (-2) + 1) * position[0].volume ) # this is to convert mt5.POSITION_TYPE_BUY (0) to +1 and mt5.POSITION_TYPE_SELL (1) to -1
            else:
                return 0
        else:
            return 0


# loads current bid and ask data
    async def get_data(self, lots_dict):

        #loop = asyncio.get_running_loop() # use the loop, that is created by asyncio.run
        tasks = [asyncio.to_thread(self.symbol_deep_quotes, symbol, lots) for symbol, lots in lots_dict.items()]
        await asyncio.gather(*tasks)


# method to calculate weighted average ask and bid price for given number of lots 
    def symbol_deep_quotes(self, symbol, lots):
        quotes = mt5.market_book_get(symbol)
        if quotes is not None:
            Order_book = pd.DataFrame(list(quotes), columns = ["type","price","volume","volume_dbl"])
            Ask_book = Order_book.loc[Order_book["type"] == 1].sort_values(by = "price", ascending = True)
            Bid_book = Order_book.loc[Order_book["type"] == 2].sort_values(by = "price", ascending = False)
            self.current_ask[symbol] = Ask_book["price"].min()
            self.current_bid[symbol] = Bid_book["price"].max()

            if lots > Ask_book["volume_dbl"].sum() or lots > Bid_book["volume_dbl"].sum():
                print(f"{datetime.now()}: Not enough bids or asks in order book for {lots} lots of {symbol}/")
                self.deep_bid[symbol] = np.nan
                self.deep_ask[symbol] = np.nan
                
            else:
                Ask_book["technical"] = Ask_book["volume_dbl"] + lots - Ask_book["volume_dbl"].cumsum()
                Ask_book = Ask_book[Ask_book["technical"]>0]
                Deep_ask = ((Ask_book[["volume_dbl","technical"]].min(axis=1) * Ask_book["price"]).sum()/lots)
                Bid_book["technical"] = Bid_book["volume_dbl"] + lots - Bid_book["volume_dbl"].cumsum()
                Bid_book = Bid_book[Bid_book["technical"]>0]
                Deep_bid = ((Bid_book[["volume_dbl","technical"]].min(axis=1) * Bid_book["price"]).sum()/lots)
                
                self.deep_bid[symbol] = Deep_bid
                self.deep_ask[symbol] = Deep_ask
        else:
            print(f"{datetime.now()}: mt5.market_book_get('{symbol}') failed, error code =", mt5.last_error())
            self.deep_bid[symbol] = np.nan
            self.deep_ask[symbol] = np.nan




# TODO: NOT USED NOW, DELETE OR AMEND
    async def run(self):
        while True:
            for symbol in self.symbols:
                await self.get_data(self.lots_dict)
                print(f"{datetime.now()}: Received data for {symbol}: bid: {self.current_bid[symbol]}, ask: {self.current_ask[symbol]}")
                await asyncio.sleep(1)

    def __del__(self):
        mt5.shutdown()
