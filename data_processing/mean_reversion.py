import asyncio
import logging
import pandas as pd
import numpy as np
import MetaTrader5 as mt5
from ib_insync import Contract
from datetime import datetime, timedelta
from config import INTERFACE_SYMBOLS, SYMBOL_DECIMALS, MOEX_MAX_LOTS, NYMEX_MAX_LOTS



class MeanReversionStrategy:
    def __init__(self, moex_connector, nymex_connector, db_handler):
        self.moex_connector = moex_connector
        self.nymex_connector = nymex_connector
        self.db = db_handler # store executed trades

        self.symbols = INTERFACE_SYMBOLS

        self.decimals = dict(zip(self.symbols, SYMBOL_DECIMALS))
        
        self.logger = logging.getLogger(__name__)
        self.moex_bid_latest, self.moex_ask_latest = None, None
        self.nymex_bid_latest, self.nymex_ask_latest = None, None
        self.counter = 0
        self.counter2 = 0

        self.active_trades = {} # (compare_prices + grid) or (check_pose + limit_grid) tasks - to check if need to stop before starting new one
        self.target_orders = {} # to use in limit_grid trading, and compare with actual moex_connector.open_orders

        # timer between moex updates
        self.timeout = 0.5 # number of seconds between cycles

        # queues and events
        self.nymex_to_moex_queue = LatestQueue() # call get_data from MOEX once there's a callback in NYMEX. TBD if replace to asyncio.Event
        self.grid_events = {symbol: asyncio.Event() for symbol in self.symbols} # trade grid strat
        self.limit_orders_events = {symbol: LatestQueue() for symbol in self.symbols} # update limit_orders
        self.limit_pose_events = {symbol: asyncio.Event() for symbol in self.symbols} # update poses and check if hedging is needed
        self.should_stop = {symbol: asyncio.Event() for symbol in self.symbols} # stop all strats for specified (or all) symbol(s)

        self.risk_coef = 0.66 # maximum amount of unhedged risk allowed as % of minimal moex lots required to be fully hedged on nymex
        # number of moex lots that can be unhedged
        self.max_risk = {}
        for symbol in self.symbols:
            moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
            self.max_risk[symbol] = self.moex_connector.lots_dict[moex_symbol] * self.risk_coef

        # max lots in limit orders on MOEX
        self.moex_max_lots = {}
        for symbol in self.symbols:
            self.moex_max_lots[symbol] = MOEX_MAX_LOTS[self.symbols.index(symbol)]

        # max lots in orders on NYMEX (for hedger)
        self.nymex_max_lots = {}
        for symbol in self.symbols:
            self.nymex_max_lots[symbol] = NYMEX_MAX_LOTS[self.symbols.index(symbol)]

        self.scheduler_on = False
        self.retcode_send = 0
        
        self.queue_in_time = {}
        self.queue_out_time = {}

# what happens when IB ticker gets updated - update current bid&ask, send queue for price updates, send events to orders and pose events
    def on_pending_tickers(self, tickers):
        ib_signal_time = datetime.now()
        for ticker in tickers:
            nymex_symbol = ticker.contract.localSymbol
            symbol = self.symbols[self.nymex_connector.symbols.index(nymex_symbol)]
            self.nymex_connector.current_bid[nymex_symbol] = ticker.bid
            self.nymex_connector.current_ask[nymex_symbol] = ticker.ask
            print(f"{datetime.now()}: Symbol {nymex_symbol}: Current bid: {self.nymex_connector.current_bid[nymex_symbol]}, Current ask: {self.nymex_connector.current_ask[nymex_symbol]}")
            self.nymex_to_moex_queue.put_nowait(self.moex_connector.symbols[self.nymex_connector.symbols.index(nymex_symbol)]) # puting in queue MOEX contract corresponding to the NYMEX contract, which was called back
            self.limit_orders_events[symbol].put_nowait(ib_signal_time) # send event/signal to update open orders
            #self.limit_pose_events[symbol].set() # send event/signal to update open positions. Not needed here as it depends on pose checking loop

# MOEX bid & ask update process. Only for market grid trading strategy:
    async def compare_prices(self, symbol):
        print(f"{datetime.now()}: comp {symbol} prices")
        asyncio.create_task(self.bot.send_message(f"{symbol} compare prices started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        while not self.should_stop[symbol].is_set():
            print(f"{datetime.now()}: Comparing prices - Start")
            await self.moex_connector.get_data({moex_symbol: self.moex_connector.lots_dict[moex_symbol]}) # TODO: review
            self.grid_events[symbol].set() # signal that data from MOEX is updated
    
            nymex_symbol = self.nymex_connector.contracts[self.moex_connector.symbols.index(moex_symbol)]
            print(f"{datetime.now()}: Received data for {moex_symbol}: bid: {self.moex_connector.current_bid[moex_symbol]}, ask: {self.moex_connector.current_ask[moex_symbol]}")
            print(f"{datetime.now()}: Received data for {moex_symbol}: deep_bid: {self.moex_connector.deep_bid[moex_symbol]}, deep_ask: {self.moex_connector.deep_ask[moex_symbol]}")
            print(f"{datetime.now()}: Received data for {nymex_symbol.localSymbol}: bid: {self.nymex_connector.current_bid[nymex_symbol.localSymbol]}, ask: {self.nymex_connector.current_ask[nymex_symbol.localSymbol]}")
            
            print(f"{datetime.now()}: Comparing prices - End")
            await asyncio.sleep(self.timeout) # TBD what should be here. THe trading logic should be done on periodic basis + on each callback from IB
        print(f"{datetime.now()}: {symbol} compare prices stopped")
        asyncio.create_task(self.bot.send_message(f"{symbol} compare prices stopped"))

# MOEX positions update process. Only for limit_grid trading strategy:
    async def check_pose(self, symbol):
        print(f"{datetime.now()}: checking positions for {symbol}")
        asyncio.create_task(self.bot.send_message(f"{datetime.now()}: {symbol} positions checking started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        self.moex_connector.load_positions([moex_symbol])
        self.nymex_connector.load_positions([nymex_symbol])
        moex_pose = self.moex_connector.positions[moex_symbol]
        #nymex_pose = self.nymex_connector.positions[nymex_symbol]
        #lots = self.moex_connector.lots_dict[moex_symbol]
        #loop = asyncio.get_running_loop()
        #nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        #nymex_symbol = nymex_contract.localSymbol 
        while not self.should_stop[symbol].is_set():
            timer = datetime.now()
            await asyncio.gather(asyncio.to_thread(self.moex_connector.load_positions, [moex_symbol]))
            # await loop.run_in_executor(None, self.nymex_connector.load_positions, [nymex_symbol]) # not needed. updated in hedger coroutine
            
            moex_pose_new = self.moex_connector.positions[moex_symbol]
            if moex_pose_new != moex_pose:
                for i in range(2):
                    self.limit_orders_events[symbol].put_nowait(None) # signal that data orders need to be updated? TODO: only call if there are changes in positions
                    self.limit_pose_events[symbol].set() # signal that data from MOEX is updated
                    print(f"{datetime.now()}: {moex_symbol} moex position changed from {moex_pose} to {moex_pose_new}.")
                    moex_pose = moex_pose_new
                    asyncio.sleep(0.9)
            #nymex_pose = self.nymex_connector.positions[nymex_symbol]
            
            # self.limit_orders_events[symbol].set() # TODO: need signal that symbol position changed compared to previous iterations..
            # open_risk = moex_pose + nymex_pose * lots
            #print(f"{datetime.now()}: check_pose: {symbol} open risk: {open_risk}.")
            print(f"{datetime.now()}: positions updated (MOEX: {moex_pose}). Time to update: {datetime.now() - timer}")
            await asyncio.sleep(self.timeout) # TBD what should be here. THe trading logic should be done on periodic basis + on each callback from IB
        print(f"{datetime.now()}: {symbol} positions checking stopped")
        asyncio.create_task(self.bot.send_message(f"{datetime.now()}: {symbol} positions checking stopped"))

# always working: getting queue from on_pending_tickers to update moex ask & bid. Not used directly in limit_grid trading?
    async def nymex_to_moex_signal(self):
        print(f"{datetime.now()}: start nymex_to_moex_signal")
        while not self.should_stop[symbol].is_set():
            moex_symbol = await self.nymex_to_moex_queue.get()
            if moex_symbol is not None:
                symbol = self.symbols[self.moex_connector.symbols.index(moex_symbol)]
                print(f"{datetime.now()}: data from NYMEX updated. Recalculating MOEX data")
                await self.moex_connector.get_data({moex_symbol: self.moex_connector.lots_dict[moex_symbol]})
                self.grid_events[symbol].set() # signal that data from MOEX is updated after the nymex callback
                print(f"{datetime.now()}: Received data for {moex_symbol}: bid: {self.moex_connector.current_bid[moex_symbol]}, ask: {self.moex_connector.current_ask[moex_symbol]}")
                print(f"{datetime.now()}: Received data for {moex_symbol}: deep_bid: {self.moex_connector.deep_bid[moex_symbol]}, deep_ask: {self.moex_connector.deep_ask[moex_symbol]}")
                # self.nymex_to_moex_queue.task_done() # not needed in a custom LatestQueue class
        print(f"{datetime.now()}: stop nymex_to_moex_signal")


# order flow - limit_grid strategy
    async def order_flow(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):
        print(f"{datetime.now()}: starting order flow for {symbol}")
        asyncio.create_task(self.bot.send_message(f"{symbol} order_flow started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        
        grid = [(i-(number_of_steps - 1)/2) for i in range(number_of_steps)]
        spread_grid = np.around([spread_step * i + mid_spread for i in grid],3)
        pose_grid = np.around([pose_step * i + mid_pose for i in grid],0)
        print(f"{datetime.now()}: {symbol} spread_grid = "+str(spread_grid)+f"\n{symbol} pose_grid = "+str(pose_grid))
        asyncio.create_task(self.bot.send_message(f"{symbol} spread_grid = "+str(spread_grid)+f"\n{symbol} pose_grid = "+str(pose_grid)))

        # check # of orders
        print(f"{datetime.now()}: should_stop signal: {self.should_stop[symbol].is_set()}")
        while not self.should_stop[symbol].is_set():
            if not self.should_stop[symbol].is_set():
                print(f"{datetime.now()}: limit_orders_events - waiting for item in queue")
                ib_signal_time = await self.limit_orders_events[symbol].get()
                if ib_signal_time is None:
                    ib_signal_time = datetime.now()
                    signal_produced_by = "pose_checker"
                else:
                    signal_produced_by = "ib_update"
                signal_received = datetime.now() - ib_signal_time
            print(f"{datetime.now()}: start checking orders, {signal_produced_by}: {signal_received} reaction time")
            self.moex_connector.load_orders(moex_symbol)
            self.moex_connector.load_positions([moex_symbol])
            # work with a dict of dataframes containing open orders:
            moex_orders = self.moex_connector.open_orders[moex_symbol][["ticket","symbol","type","volume_current", "price_open"]]

            ib_ask = self.nymex_connector.current_ask[nymex_symbol] # ask price on IB
            ib_bid = self.nymex_connector.current_bid[nymex_symbol] # bid price on IB
            moex_pose = self.moex_connector.positions[moex_symbol]
            print(f"{datetime.now()}: order_flow: moex_pose: {moex_pose}")
            pose_index = await self.searchsortedAsync(pose_grid, moex_pose, side = "left") # TODO: side - TBC. Async version - TBC
            # "id","symbol","type","volume","price

            # calculate theoretical number to keep track on cancelled orders (if they filled or not)
            #theor_pose = moex_pose + \
            #    moex_orders.loc[moex_orders["type"] == mt5.ORDER_TYPE_BUY_LIMIT, "volume_current"].sum() - \
            #        moex_orders.loc[moex_orders["type"] == mt5.ORDER_TYPE_SELL_LIMIT, "volume_current"].sum()


            orders = self.target_orders[symbol].copy()
            #cond = orders["id"]
            for i in range(number_of_steps):
                if i < pose_index:
                    # calculate desired order parameters
                    orders.loc[number_of_steps-i-1,"type"] = mt5.ORDER_TYPE_SELL_LIMIT
                    orders.loc[number_of_steps-i-1,"price_open"] = np.around(ib_ask + spread_grid[number_of_steps-i-1],self.decimals[symbol])
                    orders.loc[number_of_steps-i-1,"volume_current"] = np.around((np.min([(pose_grid[i+1] if i < (number_of_steps - 1) else moex_pose), moex_pose]) - (pose_grid[i]) ),0)
                else:
                    # calculate desired order parameters
                    orders.loc[number_of_steps-i-1,"type"] = mt5.ORDER_TYPE_BUY_LIMIT
                    orders.loc[number_of_steps-i-1,"price_open"] = np.around(ib_bid + spread_grid[number_of_steps-i-1], self.decimals[symbol])
                    orders.loc[number_of_steps-i-1,"volume_current"] = np.around(( (pose_grid[i]) - np.max([(pose_grid[i-1] if i > 0 else moex_pose), moex_pose]) ),0)
            orders = orders.sort_values(by=['price_open'], ascending=True)
            orders = orders[orders["volume_current"] != 0]
            orders["symbol"] = moex_symbol

            orders_capped = orders.copy()
            orders_capped["volume_current"] = np.minimum(orders_capped["volume_current"], self.moex_max_lots[symbol])
            self.target_orders[symbol] = orders_capped.copy()
            orders_reset = orders_capped.reset_index(names="target_index")

            # find orders to modify and cancel and resend
            
            # pandas merge() method creates new DataFrame, original ones remain unchanged
            merged = pd.merge(moex_orders, orders_reset, how = "outer", on = ['ticket', 'symbol', 'type', 'volume_current'], indicator = True)

            cancel_df = merged[merged['_merge'] == 'left_only']

            modify_df = merged[(merged['_merge'] == 'both') & (merged['price_open_x'] != merged['price_open_y'])]

            orders_to_cancel = cancel_df['ticket'].tolist()
            orders_to_modify = modify_df[['ticket', 'price_open_y']].to_dict('records')
            # time to redo.
            # place new orders only in case all orders are cancelled?
            
            print(moex_orders)
            print(f"{datetime.now()}: review orders:")
            print(orders_to_cancel)
            print(orders_to_modify)
            print(orders)
            
            cancel_tasks = [asyncio.create_task(self.cancel_task(ticket, moex_symbol, ib_signal_time, signal_produced_by)) for ticket in orders_to_cancel]
            modify_tasks = [asyncio.create_task(self.modify_task(item["ticket"], item["price_open_y"], moex_symbol, ib_signal_time, signal_produced_by)) for item in orders_to_modify]
            await asyncio.gather(*(cancel_tasks + modify_tasks))
            
            if not orders_to_cancel:
                place_df = merged[merged['_merge'] == 'right_only']
                orders_to_place = place_df["target_index"].to_list()
                print(orders_to_place)
                new_tasks = [asyncio.create_task(self.new_task(i, moex_symbol, ib_signal_time, signal_produced_by)) for i in orders_to_place]
                await asyncio.gather(*new_tasks)
                print(f"{datetime.now()}: all orders completed, time since signal = {datetime.now() - ib_signal_time}")

            else:
                print(f"{datetime.now()}: all orders completed, time since signal = {datetime.now() - ib_signal_time}")
                print(f"{datetime.now()}: check cancel orders results:")
                # need to check cancel order results
                
                cancelled_moex_orders = moex_orders.loc[moex_orders["ticket"].isin(orders_to_cancel)][["ticket","type","volume_current"]]
                print(f"{datetime.now()}: cancelled orders:")
                print(cancelled_moex_orders)

                hist_order_checker = True # to check that all cancelled orders are in the historical orders list
                hist_counter = datetime.now()
                while hist_order_checker:
                    hist_orders = self.moex_connector.load_hist_orders(moex_symbol)
                    hist_order_selected = hist_orders.loc[hist_orders["ticket"].isin(orders_to_cancel)][["ticket","type","volume_current"]]
                    selected_tickets_set = set(hist_order_selected["ticket"].unique())
                    orders_set = set(orders_to_cancel)
                    if orders_set.issubset(selected_tickets_set):
                        hist_order_checker = False
                    else:
                        print(f"{datetime.now()}: start waiting for updated historical orders results")
                        asyncio.sleep(0.05)
                        if (datetime.now() - hist_counter) > timedelta(seconds = 5):
                            print(f"{datetime.now()}: {symbol} historical orders not loaded in 5 seconds! continuing as it is")
                            asyncio.create_task(self.bot.send_message(f"{symbol} historical orders not loaded in 5 seconds! continuing as it is"))
                            #await self.stop_trading(symbol)
                            hist_order_checker = False
                            # break the loop
                        # TODO: TBA COUNTER TO BREAK THE LOOP

                
                print(f"{datetime.now()}: historical orders loaded:")
                print(hist_order_selected)

                print(f"{datetime.now()}: volumes summary for filled calcs:")
                print(f"{datetime.now()}: cancelled buy volumes {cancelled_moex_orders.loc[cancelled_moex_orders['type'] == mt5.ORDER_TYPE_BUY_LIMIT, 'volume_current'].sum()}")
                print(f"{datetime.now()}: cancelled sell volumes {cancelled_moex_orders.loc[cancelled_moex_orders['type'] == mt5.ORDER_TYPE_SELL_LIMIT, 'volume_current'].sum()}")
                print(f"{datetime.now()}: historical buy volumes {hist_order_selected.loc[hist_order_selected['type'] == mt5.ORDER_TYPE_BUY_LIMIT, 'volume_current'].sum()}")
                print(f"{datetime.now()}: historical sell volumes {hist_order_selected.loc[hist_order_selected['type'] == mt5.ORDER_TYPE_SELL_LIMIT, 'volume_current'].sum()}")
                

                filled_before_cancel = \
                    (cancelled_moex_orders.loc[cancelled_moex_orders["type"] == mt5.ORDER_TYPE_BUY_LIMIT, "volume_current"].sum() - \
                    cancelled_moex_orders.loc[cancelled_moex_orders["type"] == mt5.ORDER_TYPE_SELL_LIMIT, "volume_current"].sum()) - \
                    (hist_order_selected.loc[hist_order_selected["type"] == mt5.ORDER_TYPE_BUY_LIMIT, "volume_current"].sum() - \
                    hist_order_selected.loc[hist_order_selected["type"] == mt5.ORDER_TYPE_SELL_LIMIT, "volume_current"].sum())
                print(f"{datetime.now()}: {symbol} filled_before_cancel = {filled_before_cancel}")
                # check if need to reload positions
                if filled_before_cancel != 0:
                    positions_checker = True
                    positions_counter = datetime.now()
                    while positions_checker:
                        # loop to check that positions properly loaded after some orders filled before cancel
                        self.moex_connector.load_positions([moex_symbol])
                        moex_pose_new = self.moex_connector.positions[moex_symbol]
                        print(f"{datetime.now()}: {symbol} moex_pose_new = {moex_pose_new}, moex_pose (old) = {moex_pose}, ")
                
                        if moex_pose_new == (moex_pose + filled_before_cancel):
                            positions_checker = False
                        else:
                            await asyncio.sleep(0.05)
                            print(f"{datetime.now()}: {symbol} waiting for positions update - total 5 seconds")
                            if (datetime.now() - positions_counter) > timedelta(seconds = 5):
                                print(f"{datetime.now()}: {symbol} positions not updated in 5 seconds! continuing as it is")
                                asyncio.create_task(self.bot.send_message(f"{symbol} positions not updated in 5 seconds! continuing as it is"))
                                #await self.stop_trading(symbol)
                                positions_checker = False
                            # TODO: TBA COUNTER TO BREAK THE LOOP

                print(f"{datetime.now()}: cancel order results fully loaded")
            
            # TODO: need to calculate time to check orders. It might use too much time.
            # But for now it's useful to assign ticket numbers to target orders. If remove - need to replace the logic
            
            
            # check if all in line? TODO: what if some orders has been partially filled? or orders were modified, hence volume_initial is irrelevant?
            
            '''
            self.moex_connector.load_orders(moex_symbol)
            moex_orders_new = self.moex_connector.open_orders[moex_symbol][["ticket","symbol","type","volume_current", "price_open"]]
            moex_orders_new = moex_orders_new.astype({"ticket" : float,"type" : float})
            orders_to_check = orders_capped.copy()

            if False and (not moex_orders_new.equals(orders_to_check)):
                # not working?
                print(f"{datetime.now()}: MOEX orders after all new orders are sent")
                print(moex_orders_new)
                print(f"{datetime.now()}: target orders")
                print(orders_to_check)

                print(f"{datetime.now()}: compare method:")
                moex_orders_new['source'] = 'moex_orders_new'
                orders_to_check['source'] = 'orders_to_check'
                difference_df = pd.concat([moex_orders_new, orders_to_check]).drop_duplicates(keep=False)
                print(difference_df)
                #print(moex_orders_new.compare(orders_to_check))

                await self.bot.send_message(f"{symbol}: CHECK ORDERS ON MOEX. Need to check if the orders were partially executed while this check.")
                # need to also check if need to update / load positions on MOEX???? TODO: CONTINUE HERE

                await asyncio.sleep(0.3)
                self.limit_orders_events[symbol].clear() # duplication? TBC TODO:
                await self.stop_symbol_trading(symbol) # closing all loops as well
                break # TBC if necessary
            '''
            print(f"{datetime.now()}: orders_flow completed, time since signal = {datetime.now() - ib_signal_time}")
            # self.limit_orders_events[symbol].clear()
            # await asyncio.sleep(0.01)
        # cleaning:
        if symbol in self.target_orders:
            del self.target_orders[symbol]
        # self.limit_orders_events[symbol].clear()
        print(f"{datetime.now()}: {symbol} order_flow failed")
        asyncio.create_task(self.bot.send_message(f"{symbol} order_flow failed"))


    async def cancel_task(self, ticket, moex_symbol, ib_signal_time, signal_produced_by):
        symbol = self.symbols[self.moex_connector.symbols.index(moex_symbol)]
        print(f"{datetime.now()}: trying to cancel order # {ticket}")
        
        Keep_trying = True
        print(f"{datetime.now()}: cancelling loop:{(not self.should_stop[symbol].is_set())}")
        while (Keep_trying):
            
            order_sent_time = datetime.now() - ib_signal_time
            print(f"{datetime.now()}: cancel task starting loop to execute. {signal_produced_by}: Time since signal = {order_sent_time}")
            cancel_trade = await asyncio.to_thread(self.moex_connector.cancel_order, int(ticket))

            
            if cancel_trade.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"{datetime.now()}: moex order cancelled: {ticket}, time since signal = {datetime.now() - ib_signal_time}")
                Keep_trying = False
            elif cancel_trade.retcode == mt5.TRADE_RETCODE_MARKET_CLOSED:
                print(f"{datetime.now()}: {moex_symbol} {ticket} cancel order failure. MARKET CLOSED")
                asyncio.create_task(self.bot.send_message(f"{datetime.now()}: {moex_symbol} {ticket} cancel order failure. MARKET CLOSED"))
                await asyncio.sleep(300) # sleep [300] seconds before trying to send orders again
                if (self.should_stop[symbol].is_set()):
                    Keep_trying = False
            else:
                print(f"{datetime.now()}: {moex_symbol} {ticket} cancel order failure! retcode: {cancel_trade.retcode}, MT5 Error code: {mt5.last_error()}")
                asyncio.create_task(self.bot.send_message(f"{moex_symbol} cancel order failure! retcode: {cancel_trade.retcode}, Error code: {mt5.last_error()}"))
                Keep_trying = False



    async def modify_task(self, ticket, price, moex_symbol, ib_signal_time, signal_produced_by):
        symbol = self.symbols[self.moex_connector.symbols.index(moex_symbol)]
        Keep_trying = True
        print(f"{datetime.now()}: modifying loop:{(not self.should_stop[symbol].is_set())}")
        while (Keep_trying):
            order_sent_time = datetime.now() - ib_signal_time
            print(f"{datetime.now()}: modifying task starting loop to execute. {signal_produced_by}: Time since signal = {order_sent_time}")
            modify_trade = await asyncio.to_thread(self.moex_connector.modify_order, int(ticket), price)
            if modify_trade.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"{datetime.now()}: moex order modified: {ticket}, time since signal = {datetime.now() - ib_signal_time}")
                Keep_trying = False
            elif modify_trade.retcode == mt5.TRADE_RETCODE_MARKET_CLOSED:
                print(f"{datetime.now()}: {moex_symbol} {ticket} modify order failure. MARKET CLOSED")
                asyncio.create_task(self.bot.send_message(f"{datetime.now()}: {moex_symbol} {ticket} modify order failure. MARKET CLOSED"))
                await asyncio.sleep(300) # sleep [300] seconds before trying to send orders again
                if (self.should_stop[symbol].is_set()):
                    Keep_trying = False
            else:
                print(f"{moex_symbol} modify order failure! retcode: {modify_trade.retcode}, MT5 Error code: {mt5.last_error()}")
                asyncio.create_task(self.bot.send_message(f"{moex_symbol} modify order failure! retcode: {modify_trade.retcode}, MT5 Error code: {mt5.last_error()}"))
                Keep_trying = False



    async def new_task(self, index, moex_symbol, ib_signal_time, signal_produced_by):
        symbol = self.symbols[self.moex_connector.symbols.index(moex_symbol)]
        order_type = int(self.target_orders[symbol].loc[index, "type"])
        volume = self.target_orders[symbol].loc[index, "volume_current"]
        price = self.target_orders[symbol].loc[index, "price_open"]
        Keep_trying = True
        print(f"{datetime.now()}: new orders loop:{(not self.should_stop[symbol].is_set())}")
        while (Keep_trying):
            order_sent_time = datetime.now() - ib_signal_time
            print(f"{datetime.now()}: new task starting loop to execute. {signal_produced_by}: Time since signal = {order_sent_time}")
            moex_trade = await asyncio.to_thread(self.moex_connector.limit_order, moex_symbol, order_type, volume, price)
            if moex_trade.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"{datetime.now()}: target ticket before new order: {(self.target_orders[symbol].loc[index, 'ticket'])}")
                self.target_orders[symbol].loc[index, "ticket"] = moex_trade.order
                print(f"{datetime.now()}: target ticket after new order: {(self.target_orders[symbol].loc[index, 'ticket'])}")
                print(f"{datetime.now()}: moex order placed: {moex_trade.order}, time since signal = {datetime.now() - ib_signal_time}")
                Keep_trying = False
            elif moex_trade.retcode == mt5.TRADE_RETCODE_MARKET_CLOSED:
                print(f"{datetime.now()}: {moex_symbol} new order failure. MARKET CLOSED")
                asyncio.create_task(self.bot.send_message(f"{datetime.now()}: {moex_symbol} new order failure. MARKET CLOSED"))
                await asyncio.sleep(300) # sleep [300] seconds before trying to send orders again
                if (self.should_stop[symbol].is_set()):
                    Keep_trying = False
            else:
                print(f"{moex_symbol} new order failure! retcode: {moex_trade.retcode}, Error code: {mt5.last_error()}")
                asyncio.create_task(self.bot.send_message(f"{moex_symbol} new order failure! retcode: {moex_trade.retcode}, Error code: {mt5.last_error()}"))
                await asyncio.sleep(30) # sleep [30] seconds before trying to send orders again
            


# hedging loop
    async def hedger(self, symbol):
        print(f"{datetime.now()}: starting hedger for {symbol}")
        asyncio.create_task(self.bot.send_message(f"{symbol} hedger started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol 
        self.limit_pose_events[symbol].set
        while not self.should_stop[symbol].is_set():
            if not self.should_stop[symbol].is_set():
                await self.limit_pose_events[symbol].wait()
            # print(f"{datetime.now()}: start checking positions")
            timer = datetime.now()
            self.moex_connector.load_positions([moex_symbol])
            self.nymex_connector.load_positions([nymex_symbol])
            
            moex_pose = self.moex_connector.positions[moex_symbol]
            nymex_pose = self.nymex_connector.positions[nymex_symbol]
            lots = self.moex_connector.lots_dict[moex_symbol]
            open_risk = moex_pose + nymex_pose * lots
            print(f"{datetime.now()}: hedger: {symbol} open_risk before trade: {open_risk}")
            # apply max risk and max lots to the target nymex order lot size
            sign_open_risk = np.sign(open_risk)
            abs_open_risk_adjusted = np.trunc((abs(open_risk)+(lots-self.max_risk[symbol]))/lots)
            nymex_lots_to_trade = min(self.nymex_max_lots[symbol], abs_open_risk_adjusted)

            if nymex_lots_to_trade != 0:
                
                if sign_open_risk == 1:
                    trade_type = "Sell"
                elif sign_open_risk == -1:
                    trade_type = "Buy"
                print(f"{datetime.now()}: {symbol} starting NYMEX order execution: {trade_type} {nymex_lots_to_trade}. calcs time: {datetime.now() - timer}")
                # market order to on nymex
                nymex_trade_id = await self.nymex_connector.send_order(nymex_contract,trade_type,nymex_lots_to_trade)
                # Wait for the order to be filled
                await self.nymex_connector.order_filled_events[nymex_trade_id].wait()
                # Once the order has been filled, remove the Event
                del self.nymex_connector.order_filled_events[nymex_trade_id]
                self.moex_connector.load_positions([moex_symbol])
                self.nymex_connector.load_positions([nymex_symbol])
                moex_pose = self.moex_connector.positions[moex_symbol]
                
                #waint until IB position reflect the market order
                counter = datetime.now()
                print(f"{datetime.now()}: {symbol} nymex_pose before trade: {nymex_pose}")
                print(f"{datetime.now()}: {symbol} nymex_lots_to_trade: {nymex_lots_to_trade}")
                print(f"{datetime.now()}: {symbol} nymex loaded positions: {self.nymex_connector.positions[nymex_symbol]}")
                print((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol])
                while np.around(((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol]),0) != 0:
                    # wait until position is updated
                    print((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol])
                    await asyncio.sleep(0.1)
                    self.nymex_connector.load_positions([nymex_symbol])
                    print(f"{datetime.now()}: waiting for {symbol} IB order execution: {(datetime.now() - counter).total_seconds()} seconds")
                    if (datetime.now() - counter) > timedelta(seconds=30):
                        print(f"{datetime.now()}: {symbol} IB order not executed in 30 seconds! Stopping trading {symbol}")
                        asyncio.create_task(self.bot.send_message(f"{symbol} IB order not executed in 30 seconds! Stopping trading {symbol}"))
                        await self.stop_trading(symbol)

                nymex_pose = self.nymex_connector.positions[nymex_symbol]
                open_risk_after_trade = moex_pose + nymex_pose * lots
                print(f"{datetime.now()}: {symbol}: {trade_type} {nymex_lots_to_trade} IB lot(s) to close open risk of {open_risk} lots"+f"\n\
                    open risk after trade: {open_risk_after_trade} lots. MOEX lots: {moex_pose}, NYMEX lots: {nymex_pose}. Function run time: {datetime.now() - timer}")
                asyncio.create_task(self.bot.send_message(f"{symbol}: {trade_type} {nymex_lots_to_trade} IB lot(s) to close open risk of {open_risk} lots"+f"\n\
                    open risk after trade: {open_risk_after_trade} lots. MOEX lots: {moex_pose}, NYMEX lots: {nymex_pose}"))

            else:
                # clear signal from check_pose loop only in case open risk is below the max_risk limit
                self.limit_pose_events[symbol].clear()
                print(f"{datetime.now()}: hedger: {symbol} open risk: {open_risk}. Function run time: {datetime.now() - timer}")
            # don't clear limit_pose_events until open_risk below max_risk.
            await asyncio.sleep(1) # To be reduced
        self.limit_pose_events[symbol].clear()
        print(f"{datetime.now()}: {symbol} hedger failed")
        asyncio.create_task(self.bot.send_message(f"{symbol} hedger failed"))





# trading strategies below

# command to start grid trading
    async def start_grid_trading(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):
        print(f"{datetime.now()}: starting market grid trading:")
        #moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol         
        await self.nymex_connector.subscribe_bid_ask(nymex_contract)
        print(f"{datetime.now()}: Symbol {nymex_symbol}: Current bid: {self.nymex_connector.current_bid[nymex_symbol]}, Current ask: {self.nymex_connector.current_ask[nymex_symbol]}")
        print(f"{datetime.now()}: subscription to NYMEX {nymex_symbol} completed")


        if symbol in self.active_trades:
            # There is already an active task for this symbol, so we stop it before starting a new one
            print(f"{datetime.now()}: Stopping the active trading task for {symbol} before starting a new one.")
            self.should_stop[symbol].set()
            await asyncio.sleep(self.timeout+self.timeout) # not sure if needed
            self.grid_events[symbol].set()
            await asyncio.gather(*self.active_trades[symbol])
            self.grid_events[symbol].clear()

        self.should_stop[symbol].clear()
        coroutines = [self.compare_prices(symbol), self.grid(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose), self.nymex_to_moex_signal]
        self.active_trades[symbol] = [asyncio.create_task(coro) for coro in coroutines]
        if not self.scheduler_on:
            asyncio.create_task(self.scheduler())
        await asyncio.gather(*self.active_trades[symbol])

# command to start limit trading
    async def start_limit_trading(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):
        print(f"{datetime.now()}: starting limit grid trading:")
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        await self.nymex_connector.subscribe_bid_ask(nymex_contract)
        print(f"{datetime.now()}: Symbol {nymex_symbol}: Current bid: {self.nymex_connector.current_bid[nymex_symbol]}, Current ask: {self.nymex_connector.current_ask[nymex_symbol]}")
        print(f"{datetime.now()}: subscription to NYMEX {nymex_symbol} completed")
        
        if symbol in self.active_trades:
            # There is already an active task for this symbol, so we stop it before starting a new one
            print(f"{datetime.now()}: Stopping the active trading task for {symbol} before starting a new one.")
            self.should_stop[symbol].set()
            await asyncio.sleep(self.timeout+self.timeout) # not sure if needed
            self.limit_pose_events[symbol].set()
            self.limit_orders_events[symbol].put_nowait(None)

            self.limit_pose_events[symbol].clear()
            await asyncio.gather(*self.active_trades[symbol])

            for task in self.active_trades[symbol]:
                print(f"{datetime.now()}: cancelling the task")
                task.cancel()
        print(f"{datetime.now()}: clearing stop signal:")
        self.should_stop[symbol].clear()
        print(f"{datetime.now()}: stop signal cleared.")
        
        self.target_orders[symbol] = pd.DataFrame(columns = ["ticket","symbol","type","volume_current", "price_open"], ).astype({"ticket" : int, "symbol" : str, "type" : int, "volume_current" : float, "price_open" : float})
        
        self.active_trades[symbol] = [
            asyncio.create_task(self.check_pose(symbol)), 
            asyncio.create_task(self.order_flow(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)),
            asyncio.create_task(self.hedger(symbol))
        ]
        if not self.scheduler_on:
            asyncio.create_task(self.scheduler())
        await asyncio.gather(*self.active_trades[symbol])

# stop all trading for specific symbol or all symbols at once
    async def stop_trading(self,symbol=None):
        if symbol is None:
            # Stop all running tasks for each symbol
            print(f"{datetime.now()}: stop_trading:")
            for symbol_to_cancel in self.active_trades:
                print(f"{datetime.now()}: stop_trading: {symbol_to_cancel}")
                await self.stop_symbol_trading(symbol_to_cancel)
                self.grid_events[symbol_to_cancel].set()
                self.limit_orders_events[symbol_to_cancel].put_nowait(None)
                
                try:
                    self.nymex_to_moex_queue[symbol_to_cancel].put_nowait(None)
                except:
                    pass
                self.limit_pose_events[symbol_to_cancel].set()
                
                self.grid_events[symbol_to_cancel].clear()
                self.limit_pose_events[symbol_to_cancel].clear()

                await asyncio.gather(*self.active_trades[symbol_to_cancel])

                moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol_to_cancel)]
                print(self.moex_connector.open_orders[moex_symbol])
                if moex_symbol in self.moex_connector.open_orders:
                    print(self.moex_connector.open_orders[moex_symbol])
                    for ticket in self.moex_connector.open_orders[moex_symbol]["ticket"]:
                        cancel_trade = await asyncio.to_thread(self.moex_connector.cancel_order, int(ticket))
                        if cancel_trade.retcode == mt5.TRADE_RETCODE_DONE:
                            print(f"{datetime.now()}: moex order cancelled: {ticket}")
                        else:
                            print(f"{datetime.now()}: {moex_symbol} cancel order failure! Error code: {mt5.last_error()}")
                            asyncio.create_task(self.bot.send_message(f"{moex_symbol} cancel order failure! retcode: {cancel_trade.retcode}.Error code: {mt5.last_error()}"))
                    del self.moex_connector.open_orders[moex_symbol]
                del self.active_trades[symbol_to_cancel] # TODO: check if working properly
        else:
            if symbol in self.active_trades:
                # Stop the task for the specified symbol
                await self.stop_symbol_trading(symbol)
                self.grid_events[symbol].set()
                self.limit_orders_events[symbol].put_nowait(None)
                try:
                    self.nymex_to_moex_queue[symbol_to_cancel].put_nowait(None)
                except:
                    pass
                self.limit_pose_events[symbol].set()
                
                self.grid_events[symbol].clear()
                self.limit_pose_events[symbol].clear()

                await asyncio.gather(*self.active_trades[symbol])

                moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
                if moex_symbol in self.moex_connector.open_orders:
                    for ticket in self.moex_connector.open_orders[moex_symbol]["ticket"]:
                        cancel_trade = await asyncio.to_thread(self.moex_connector.cancel_order, int(ticket))
                        if cancel_trade.retcode == mt5.TRADE_RETCODE_DONE:
                            print(f"{datetime.now()}: moex order cancelled: {ticket}")
                        else:
                            print(f"{datetime.now()}: {moex_symbol} cancel order failure! Error code: {mt5.last_error()}")
                            asyncio.create_task(self.bot.send_message(f"{moex_symbol} cancel order failure! retcode: {cancel_trade.retcode}. Error code: {mt5.last_error()}"))
                    del self.moex_connector.open_orders[moex_symbol]
                del self.active_trades[symbol]
            else:
                print(f"{datetime.now()}: {symbol} is not in active_trades")
                asyncio.create_task(self.bot.send_message(f"{symbol} is not in active_trades"))


    async def stop_symbol_trading(self,symbol):
        self.should_stop[symbol].set()
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        await self.nymex_connector.unsubscribe_bid_ask(nymex_contract)


# strategy for grid trading via market orders. This should be stopped before limit_grid trading.
    async def grid(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):

        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol

        self.moex_connector.load_positions([moex_symbol])
        self.nymex_connector.load_positions([nymex_symbol])
        
        moex_pose = self.moex_connector.positions[moex_symbol]
        nymex_pose = self.nymex_connector.positions[nymex_symbol]
        
        lots = self.moex_connector.lots_dict[moex_symbol]

        grid = [(i-(number_of_steps - 1)/2) for i in range(number_of_steps)]
        spread_grid = np.around([spread_step * i + mid_spread for i in grid],3)
        pose_grid = np.around([pose_step * i + mid_pose for i in grid],0)
        asyncio.create_task(self.bot.send_message(f"{symbol} spread_grid = "+str(spread_grid)+f"\n{symbol} pose_grid = "+str(pose_grid)))
        self.grid_events[symbol].set
        while not self.should_stop[symbol].is_set():
            # Wait for the trade event to be set
            if not self.should_stop[symbol].is_set():
                await self.grid_events[symbol].wait()
            # await self.bot.send_message(f"{symbol} spread_grid iteration started - TEST")
            # check if real positions equals to calculated positions

            while (not moex_pose == (- nymex_pose * lots)) and (not moex_pose == self.moex_connector.positions[moex_symbol]) and (not nymex_pose == self.nymex_connector.positions[nymex_symbol]):
                # print(f"{datetime.now()}: CHECK POSITION ON MOEX AND NYMEX!!!!!!!!!!!!!!!!!!! \nExiting the trade loop")
                # await self.bot.send_message("CHECK POSITION ON MOEX AND NYMEX\nExiting the trade loop")
                asyncio.create_task(self.bot.send_message(f"{symbol}: CHECK POSITIONS ON MOEX AND NYMEX!"))
                await asyncio.sleep(0.3)

            print(f"{datetime.now()}: pose on MOEX: " + str(moex_pose))
            print(f"{datetime.now()}: pose on NYMEX: " + str(nymex_pose))            
            ib_ask = self.nymex_connector.current_ask[nymex_symbol] # ask price on IB
            ib_bid = self.nymex_connector.current_bid[nymex_symbol] # bid price on IB
            moex_ask = self.moex_connector.deep_ask[moex_symbol] # ask price on MOEX
            moex_bid = self.moex_connector.deep_bid[moex_symbol] # bid price on MOEX
            if (not np.isnan(ib_ask)) and (not np.isnan(moex_ask)) and (not np.isnan(ib_bid)) and (not np.isnan(moex_bid)):
                # calculate positive (moex price > NYMEX price) spread before slippage to enter: sell moex & buy nymex
                Spread_to_short = moex_bid - ib_ask
                # calculate positive (moex price > NYMEX price) spread before slippage to exit: buy moex & sell nymex
                Spread_to_long = moex_ask - ib_bid
                print(f"{datetime.now()}: {symbol}: ",Spread_to_long,Spread_to_short)
                spread_to_long_index = await self.searchsortedAsync(spread_grid,Spread_to_long,side='left')
                spread_to_short_index = await self.searchsortedAsync(spread_grid,Spread_to_short,side='left')
                pose_to_short_index = await self.searchsortedAsync(pose_grid,moex_pose,side='left')
                pose_to_long_index = await self.searchsortedAsync(pose_grid,moex_pose,side='right')
                if ((number_of_steps - spread_to_short_index) < pose_to_short_index) and ((spread_to_short_index)>1):
                    print(f"{datetime.now()}: {symbol}: trying to short moex and long IB")
                    #  trade short
                    # market order to sell 25 lots on moex
                    moex_trade = self.moex_connector.send_order(moex_symbol,"Sell",lots)
                    if moex_trade.retcode == mt5.TRADE_RETCODE_DONE:
                        # market order to buy 1 lot on nymex
                        nymex_trade_id = await self.nymex_connector.send_order(nymex_contract,"Buy",1)
                        # Wait for the order to be filled
                        await self.nymex_connector.order_filled_events[nymex_trade_id].wait()
                        # Once the order has been filled, remove the Event
                        del self.nymex_connector.order_filled_events[nymex_trade_id]
                        # TODO: store NYMEX trade results / pass to store trade instance for further storage to sqlite below
                        # store trade results into the database
                        # TODO: Storage logic - create asyncio task for trading results.
                        
                        # print or send or report the trades on terminal and to Telegram (to be developed)
                        print(f"{datetime.now()}: {symbol}: sell moex buy IB with target value"+str(Spread_to_short))
                        nymex_pose = nymex_pose + 1 # TODO: this should check the real pose?
                        moex_pose = moex_pose - lots # TODO: this should check the real pose?
                        self.moex_connector.load_positions([moex_symbol])
                        self.nymex_connector.load_positions([nymex_symbol])
                        asyncio.create_task(self.bot.send_message(f"{symbol}: sell moex buy IB with target value "+str(np.around(Spread_to_short,3))+f"\n\
                            {symbol} pose on MOEX: " + str(moex_pose)+f"\n{symbol} pose on NYMEX: " + str(nymex_pose)))
                    else:
                        print(f"{datetime.now()}: {symbol} order_send failed, retcode={moex_trade.retcode}")
                        if self.retcode_send == 1:
                            asyncio.create_task(self.bot.send_message(f"{symbol}1: order_send failed, retcode={moex_trade.retcode}"))

                elif (number_of_steps - spread_to_long_index > pose_to_long_index) and ((number_of_steps - spread_to_long_index) > 1):
                    print(f"{datetime.now()}: {symbol}: trying to long moex and short IB")
                    # trade long
                    # market order to buy # lots on moex
                    moex_trade = self.moex_connector.send_order(moex_symbol,"Buy",lots)
                    if moex_trade.retcode == mt5.TRADE_RETCODE_DONE:
                        # market order to sell 1 lot on nymex
                        nymex_trade_id = await self.nymex_connector.send_order(nymex_contract,"Sell",1)
                        # Wait for the order to be filled
                        await self.nymex_connector.order_filled_events[nymex_trade_id].wait()
                        # Once the order has been filled, remove the Event
                        del self.nymex_connector.order_filled_events[nymex_trade_id]                        

                        # print or send or report the trades on terminal and to Telegram (to be developed)
                        print(f"{datetime.now()}: {symbol}: buy moex sell IB with target value"+str(Spread_to_long))
                        nymex_pose = nymex_pose - 1 # TODO: this should check the real pose?
                        moex_pose = moex_pose + lots # TODO: this should check the real pose?
                        self.moex_connector.load_positions([moex_symbol])
                        self.nymex_connector.load_positions([nymex_symbol])
                        asyncio.create_task(self.bot.send_message(f"{symbol}: buy moex sell IB with target value "+str(np.around(Spread_to_long,3))+f"\n\
                            {symbol} pose on MOEX: " + str(moex_pose)+f"\n{symbol} pose on NYMEX: " + str(nymex_pose)))
                    else:
                        print(f"{datetime.now()}: {symbol}: order_send failed, retcode={moex_trade.retcode}")
                        if self.retcode_send == 1:
                            asyncio.create_task(self.bot.send_message(f"{symbol}: order_send failed, retcode={moex_trade.retcode}"))

                else:
                    print(f"{datetime.now()}: {symbol}: no trades this iteration")
                # Reset the event so we can wait for it again
                self.grid_events[symbol].clear()
                 
            else:
                self.grid_events[symbol].clear()
                print(f"{datetime.now()}: no access to bid and ask data")
                asyncio.create_task(self.bot.send_message("no access to bid and ask data"))
                await asyncio.sleep(30)
            
        self.grid_events[symbol].clear()
        print(f"{datetime.now()}: {symbol} grid loop failed")
        asyncio.create_task(self.bot.send_message(f"{symbol} grid loop failed"))


# time scheduler

    async def scheduler(self):
        now = datetime.now()
        stop_time = now.replace(hour = 23, minute = 45, second = 0, microsecond=0)  # 23:45 UTC
        start_time = now.replace(hour = 9, minute = 0, second = 1, microsecond=0)  # 9:01:05 UTC
        print(f"{datetime.now()}: starting scheduler. start_time = {start_time.time()}. stop_time = {stop_time.time()}")
        asyncio.create_task(self.bot.send_message(f"starting scheduler. start_time = {start_time.time()}. stop_time = {stop_time.time()}"))
        self.scheduler_on = True
        while self.scheduler_on and self.active_trades:
            current_time = datetime.now()
            print(f"{datetime.now()}: checking time for scheduler")
            if start_time <= current_time < stop_time:
                # TODO: add restart of the previously closed task
                print(f"{datetime.now()}: time inside the working hours")
                pass
            else:
                # stop all trading
                print(f"{datetime.now()}: time outside the working hours")
                self.scheduler_on = False
                await self.stop_trading()

            await asyncio.sleep(180)  # Check every 3 minutes
        print(f"{datetime.now()}: scheduler stopped at {datetime.now().time()}")
        asyncio.create_task(self.bot.send_message(f"scheduler stopped at {datetime.now().time().replace(microsecond=0)}"))




# TODO: TBD if need to rewrite for asynchonous execution
    async def searchsortedAsync(self, list_grid,float_number,side:str):
        return np.searchsorted(list_grid,float_number,side)

# connections with user_interface telegram bot

    def set_bot(self, bot):
        self.bot = bot

# RUN function:
    async def run(self):
        self.nymex_connector.ib.pendingTickersEvent  += self.on_pending_tickers
        #tasks = [asyncio.create_task(self.nymex_to_moex_signal())]
        #await asyncio.gather(*tasks)


class LatestQueue(asyncio.Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get(self):
        # This will get the latest item and clear all older items
        while True:
            item = await super().get()
            try:
                if self.empty():
                    return item
            finally:
                self.task_done()
