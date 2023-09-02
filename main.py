import asyncio
import sys
#import yappi # profiler compatible with asyncio / multithreading / gevent
from moex_connection import MoexAsyncWrapper
from nymex_connection import NymexAsyncWrapper
from iqfeed_connection import IQFeedLevel1Listener, iqfeed_wrapper
from data_processing import MeanReversionStrategy
from user_interface import TelegramBot, WebInterface
from database import DatabaseHandler
from logging_and_notification import Logger
from config import *
from datetime import datetime

from pyiqfeed import FeedConn, QuoteConn

async def main():
    # start printing and saving terminal output to a file
    logger = Logger()
    print(f"{datetime.now()}: Starting ComArb_Project")

    loop = asyncio.get_running_loop() # The loop
    # storage_queue = asyncio.Queue() # Queue for storage tasks?
    # Alternatively just start tasks and don't care about queue as it's already built int into aiosqlite <- this

    # Initialize database
    db_handler = DatabaseHandler()
    # await until the databes fully initialize
    await db_handler.wait_until_ready()

    # Initialize MOEX and NYMEX connections
    moex_conn = MoexAsyncWrapper(MOEX_LOGIN, MOEX_PASSWORD, MOEX_SERVER, MOEX_SYMBOLS, MOEX_LOTS)
    await moex_conn.initialize()
    nymex_conn = NymexAsyncWrapper(NYMEX_API_KEY, NYMEX_HOST, NYMEX_PORT, NYMEX_CONTRACTS)
    await nymex_conn.initialize()
    print("1")
    
    #iqfeed_listener = IQFeedLevel1Listener("Level 1 Listener")
    print("2")
    iqfeed_conn = iqfeed_wrapper()
    print("3")
    trading = MeanReversionStrategy(moex_conn, nymex_conn, iqfeed_conn, db_handler)
    print("4")
    #await trading.initialize()

    # Initialize user interfaces (Telegram bot and/or web interface)
    telegram_bot = TelegramBot(trading, TELEGRAM_API_TOKEN, TELEGRAM_CHAT_IDS)
    #web_interface = WebInterface(WEB_SERVER_HOST, WEB_SERVER_PORT)
    print("5")
    # Run the asyncio event loop
    tasks = [
        #moex_conn.run(), # TBC if needed
        #nymex_conn.run(), # TBC if needed
        #asyncio.create_task(iqfeed_conn.run()),
        #asyncio.create_task(trading.run()),
        asyncio.create_task(telegram_bot.run()),
        #asyncio.create_task(web_interface.run())
    ]
    print("6")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        #yappi.set_clock_type("WALL")
        #with yappi.run():
        asyncio.run(main(), debug = True)
        #func_stats = yappi.get_func_stats()
        #sorted_func_stats = func_stats.sort("totaltime")
        #sorted_func_stats.print_all(out=None, limit=20)

        #thread_stats = yappi.get_thread_stats()
        #sorted_thread_stats = thread_stats.sort("totaltime")
        #sorted_thread_stats.print_all(out=None, limit=20)


    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        sys.stdout.close()  # Close the log file
        '''
        orders flow should be recalc
        TODO: Need to track positions based on orders. Loading positions is slow and leads to errors.
        TODO:    positions checking should trigger order_flow only in case there are changes in positions
        TODO:    no need to do deep bid/ask calcs in case of limit orders flow strategy
        (added message info) Need to control that all required coroutines are running (order flow + Hedger!). Check if both started working, or one of them stopped working
        Check if on_pending_tickers should work always or be started / closed when stopping the tasks/strategies.
        (done) IB orders - declined frequent transactions? Split into larger traders.

        (added the logic - Need testing!) orders filled but positions not updated leading to dublicate order sent and executed - added 0.01 sleep time to cycle.
        
        (order_flow - cancel / modify / new) Need to fire orders and wait for all of them, instead of iterating with waiting for each
        e.g. create_task on each, than await on asyncio.gather

        !check dataframe objects and variable assignments
        
        need resuts of orders executions (fifo?) - including timestamps and prices. with further saving into database

        ?
        orders not updated even when positions are flat - 
        result random small amount orders in opposite direction to the real ones 
        (e.g. bought 200 at ~currentp price, then sold 33 for no reason)
        might be that volume current or original - somewhere not correct chosen / used


        ib orders - checking and reporting?
        all relevant timestamps:
            0. ib_signal_time signal (ib related only)
            1. signal_received moment of updated price from IB or moex
            2. order_sent moment to send orders
            3. moment orders are received
            4. moment orders are executed/filled
            5. moment order confirmation is received
            
            Latencies for MOEX and IB:
                0-1. Data latency  (IB only)
                1-2. processing latency
                2-3. order rounting latency
                3-4. settlement latency

        check market open in MOEX and NYMEX
        check margin requirements in NYMEX
        check all info on spreads / positions etc.
        
        protect against several bot running from different places -_-

        cybersecurity, esp. with web app
        
        '''