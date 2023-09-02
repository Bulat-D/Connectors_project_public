# testing pyiqfeed

import pyiqfeed as iq
import numpy as np
from typing import Sequence, List, Tuple
from datetime import datetime, timedelta
from pyiqfeed import FeedConn, QuoteConn
from config import IQFEED_SYMBOLS
# nymex_async_wrapper.py
import asyncio

#####################################################

class IQFeedLevel1Listener(iq.SilentIQFeedListener):

    def __init__(self, name: str):
        super().__init__(name)

        self.symbols = IQFEED_SYMBOLS
        self.current_ask = {}
        self.current_bid = {}
        for symbol in self.symbols:
            self.current_ask[symbol] = None
            self.current_bid[symbol] = None


    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol

        :param bad_symbol: The bad symbol

        """
        print("IQFeed invalid symbol. Probably no subscription to COMEX!!!")
        pass

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        """
        A news story hit the news wires.

        :param news_item: NewsMsg namedtuple .

           The elements of each HeadlineMsg are:
          'distributor': News Source
          'story_id': ID of the story. Used to get full text
          'symbol_list': Symbols that are affected by the story
          'story_time': When the story went out
          'headline': The story's headline

        If you want the full text, get it using NewsConn using story_id.

        """
        pass

    def process_regional_quote(self, quote: np.array) -> None:
        """
        The top of book at a market-center was updated

        :param quote: numpy structured array with the actual quote

        dtype of quote is QuoteConn.regional_type

        """
        pass

    def process_summary(self, summary: np.array) -> None:
        """
        Initial data after subscription with latest quote, last trade etc.

        :param summary: numpy structured array with the data.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        pass

    def process_update(self, update: np.array) -> None:
        """
        Update with latest quote, last trade etc.

        :param update: numpy structured array with the data.

        Compare with prior cached values to find our what changed. Nothing may
        have changed.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        iqfeed_signal_time = datetime.now()
        symbol = update[0]["Symbol"].astype(str)

        if (update[0]["Ask"] != self.current_ask[symbol]) or (update[0]["Bid"] != self.current_bid[symbol]):
            
            print(f"Ask/Bid data received on: {datetime.now()}")
           
            self.current_ask[symbol] = update[0]['Ask']
            self.current_bid[symbol] = update[0]['Bid']
            print(f"iqfeed update bid/ask: bid={self.current_bid[symbol]}, ask={self.current_ask[symbol]}")
            self.trading.on_iqfeed_update(symbol, iqfeed_signal_time)

        pass


    def process_fundamentals(self, fund: np.array) -> None:
        """
        Message with information about symbol which does not change.

        :param fund: numpy structured array with the data.

        Despite the word fundamentals used to describe this message in the
        IQFeed docs and the name of this function, you don't get just
        fundamental data. You also get reference date like the expiration date
        of an option.

        Called once when you first subscribe and every time you request a
        refresh.

        """
        pass

    def process_auth_key(self, key: str) -> None:
        """Authorization key: Ignore unless you have a good reason not to."""
        pass

    def process_keyok(self) -> None:
        """Relic from old authorization mechanism. Ignore."""
        pass

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        """
        Information about your entitlements etc.

        :param cust_info: The data as a named tuple

        Useful to look at if you are getting delayed data when you expect
        real-time etc.

        """
        pass

    def process_watched_symbols(self, symbols: Sequence[str]) -> None:
        """List of all watched symbols when requested."""
        pass

    def process_log_levels(self, levels: Sequence[str]) -> None:
        """List of current log levels when requested."""
        pass

    def process_symbol_limit_reached(self, sym: str) -> None:
        """
        Subscribed to more than the number of symbols you are authorized for.

        :param sym: The subscription which took you over the limit.

        """
        pass

    def process_ip_addresses_used(self, ip: str) -> None:
        """IP Address used to connect to DTN's servers."""
        pass

    def set_trading(self, trading):
        self.trading = trading

###############

class iqfeed_wrapper:
    def __init__(self):
        self.quote_conn = None
        self.listener = IQFeedLevel1Listener("Level 1 Listener")
        self.quote_conn = iq.QuoteConn(name="pyiqfeed-Example-lvl1")
        self.quote_conn.add_listener(self.listener)

        self.symbols = IQFEED_SYMBOLS
        print(f"{datetime.now()}: starting connecting to IQFeed (Ask / Bid)")
        
        self.quote_conn.connect()
        self.quote_conn.select_update_fieldnames(["Ask","Bid"])
        print(f"{datetime.now()}: self.quote_conn.connect() done")
        pass

    async def subscribe(self, symbol: str):

        # load current bid / ask
        print(f"{datetime.now()}: connecting to history Connector to IQFeed")
        history_con = iq.HistoryConn(name="history-conn-test")
        history_con.connect()
        print(f"{datetime.now()}: requesting bid/ask snapshot")
        ticks = history_con.request_ticks(ticker=symbol, max_ticks=4)

        self.listener.current_bid[symbol] = ticks[0][8]
        self.listener.current_ask[symbol] = ticks[0][9]
        print(f"{datetime.now()}: subscribing to {symbol} in IQFeed")
        self.quote_conn.watch(symbol)
        print(f"{datetime.now()}: iqfeed current bid/ask updated")
        hist_disc_task = asyncio.to_thread(history_con.disconnect)
        return hist_disc_task

    def unsubscribe(self, symbol: str):
        print(f"{datetime.now()}: stop receiving from IQFeed")
        self.quote_conn.unwatch(symbol)


    def run(self):
        print("run iqfeed started")
        #self.quote_conn = iq.QuoteConn(name="pyiqfeed-Example-lvl1")
        #self.quote_conn.add_listener(self.listener)
        print("run iqfeed ended")

    def __del__(self):
        self.quote_conn.disconnect()

