import asyncio
import pandas as pd
import numpy as np
import MetaTrader5 as mt5
from ib_insync import Contract
from datetime import datetime, timedelta
from config import INTERFACE_SYMBOLS, SYMBOL_DECIMALS, MOEX_MAX_LOTS, NYMEX_MAX_LOTS



class MeanReversionStrategy:
    def __init__(self, moex_connector, nymex_connector, iqfeed_connector, db_handler):
        print("start initializing meanreversion")
        self.moex_connector = moex_connector
        self.nymex_connector = nymex_connector
        self.iqfeed_connector = iqfeed_connector
        print(11)
        self.iqfeed_connector.listener.set_trading(self) # establish connection from iqfeed listener to mean_reversion..?
        print(22)
        self.db = db_handler # store executed trades

        self.symbols = INTERFACE_SYMBOLS

        self.decimals = dict(zip(self.symbols, SYMBOL_DECIMALS))
        
        self.pose = {}
        self.iqfeed_bid = {} # TODO: replace by links to iqfeed_connector??
        self.iqfeed_ask = {}
        print(33)
        self.target_orders = {} # to use in limit_grid trading
        for symbol in self.symbols:
            self.iqfeed_bid[symbol] = np.nan
            self.iqfeed_ask[symbol] = np.nan
            self.pose[symbol] = self.moex_connector.load_position(symbol)
            self.target_orders[symbol] = {
                "buy_vol": None,
                "buy_spr": None,
                "buy_ticket": None,
                "sell_vol": None,
                "sell_spr": None,
                "sell_ticket": None
            }
        print(self.target_orders)
        print(self.target_orders["gas1"]["sell_ticket"])
        print(44)
        
        self.pose_grid = {}
        self.spread_grid = {}
        self.number_of_steps = {}

        self.iqfeed_task = {}

        self.counter = 0

        self.active_trades = {} # (compare_prices + grid) or (check_pose + limit_grid) tasks - to check if need to stop before starting new one

        # timer between moex updates
        self.timeout = 0.25 # number of seconds between cycles

        # queues and events
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

        self.nymex_connector.ib.pendingTickersEvent  += self.on_pending_tickers
        



# IB tick updates - exclusively for a Gold contract
    def on_pending_tickers(self, tickers):
        ib_signal_time = datetime.now()
        for ticker in tickers:
            nymex_symbol = ticker.contract.localSymbol
            if nymex_symbol == self.nymex_connector.symbols[2]:
                print(f"{datetime.now()}: on_pending tickers: starting, reaction time: {datetime.now()-ib_signal_time}")
                symbol = self.symbols[self.nymex_connector.symbols.index(nymex_symbol)]
                moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
                self.nymex_connector.current_bid[nymex_symbol] = ticker.bid
                self.nymex_connector.current_ask[nymex_symbol] = ticker.ask
                print(f"{datetime.now()}: on_pending tickers: Symbol {nymex_symbol}: Current bid: {self.nymex_connector.current_bid[nymex_symbol]}, Current ask: {self.nymex_connector.current_ask[nymex_symbol]}")
                
                buy_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["buy_ticket"]) # returns None if no such order exists
                sell_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["sell_ticket"]) # returns None if no such order exists
                print(f"{datetime.now()}: on_pending tickers: orders loaded")

                new_buy_price = self.nymex_connector.current_bid[nymex_symbol] + self.target_orders[symbol]["buy_spr"]
                new_sell_price = self.nymex_connector.current_ask[nymex_symbol] + self.target_orders[symbol]["sell_spr"]
                
                modify_buy_failed, modify_buy_failed = False, False
                print(f"{datetime.now()}: on_pending tickers: start checking orders to modify:")
                if buy_order:
                    if new_buy_price != buy_order[0].price_open:
                        print(f"{datetime.now()}: on_pending tickers: sending modify buy order")
                        modify_buy_order = self.moex_connector.modify_order(self.target_orders[symbol]["buy_ticket"], new_buy_price)
                        print(f"{datetime.now()}: on_pending tickers: modify buy order sent")
                        if modify_buy_order.retcode == mt5.TRADE_RETCODE_INVALID:
                            modify_buy_failed = True
                print(f"{datetime.now()}: on_pending tickers: buy order checked")
                if sell_order:
                    if new_sell_price != sell_order[0].price_open:
                        print(f"{datetime.now()}: on_pending tickers: sending modify bselluy order")
                        modify_sell_order = self.moex_connector.modify_order(self.target_orders[symbol]["sell_ticket"], new_sell_price)
                        print(f"{datetime.now()}: on_pending tickers: modify sell order sent")
                        if modify_sell_order.retcode == mt5.TRADE_RETCODE_INVALID:
                            modify_buy_failed = True


                print(f"{datetime.now()}: on_pending tickers: modify orders done, completiom time: {datetime.now()-ib_signal_time}")
                if modify_buy_failed or modify_buy_failed:
                    print(f"{datetime.now()}: on_pending tickers: modify orders failed - on_filled order will be called (this is outside)")
                    self.on_filled_order(symbol, self.pose_grid[symbol], self.spread_grid[symbol],self.number_of_steps[symbol])
                    print(f"{datetime.now()}: on_pending tickers: modify orders failed - on_filled order completed (this is outside). time since signal: {datetime.now()-ib_signal_time}")

                print(f"{datetime.now()}: on_pending tickers: completed, total time: {datetime.now()-ib_signal_time}")

# IQFeed tick updates - modify orders - not for gold, as COMEX is not included into IQFeed subscription (10/08/23)
    def on_iqfeed_update(self, iqfeed_symbol, iqfeed_signal_time):
        print(f"{datetime.now()}: on_iqfeed_update: starting, reaction time: {datetime.now()-iqfeed_signal_time}")
        symbol = self.symbols[self.iqfeed_connector.symbols.index(iqfeed_symbol)]
        self.iqfeed_bid[symbol] = self.iqfeed_connector.listener.current_bid[iqfeed_symbol]
        self.iqfeed_ask[symbol] = self.iqfeed_connector.listener.current_ask[iqfeed_symbol]
        print(f"{datetime.now()}: on_iqfeed_update: {symbol} Current bid: {self.iqfeed_bid[symbol]}, current ask: {self.iqfeed_ask[symbol]}")
        print(f"self.target_orders[symbol]['sell_ticket']: {self.target_orders[symbol]['sell_ticket']}")
        buy_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["buy_ticket"]) # returns None if no such order exists
        sell_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["sell_ticket"]) # returns None if no such order exists
        print(f"{datetime.now()}: on_iqfeed_update: orders loaded")
        modify_order_failed = False
        print(f"{datetime.now()}: on_iqfeed_update: start checking orders to modify:")
        if buy_order:
            new_buy_price = self.iqfeed_bid[symbol] + self.target_orders[symbol]["buy_spr"]
            if new_buy_price != buy_order[0].price_open:
                print(f"{datetime.now()}: on_iqfeed_update: sending modify buy order")
                modify_buy_order = self.moex_connector.modify_order(self.target_orders[symbol]["buy_ticket"], new_buy_price)
                print(f"{datetime.now()}: on_iqfeed_update: modify buy order sent")
                if modify_buy_order.retcode == mt5.TRADE_RETCODE_INVALID:
                    modify_order_failed = True
        print(f"{datetime.now()}: on_iqfeed_update: buy order checked")
        if sell_order:
            new_sell_price = self.iqfeed_ask[symbol] + self.target_orders[symbol]["sell_spr"]
            if new_sell_price != sell_order[0].price_open:
                print(f"{datetime.now()}: on_iqfeed_update: sending modify order")
                modify_sell_order = self.moex_connector.modify_order(self.target_orders[symbol]["sell_ticket"], new_sell_price)
                print(f"{datetime.now()}: on_iqfeed_update: modify sell order sent")
                if modify_sell_order.retcode == mt5.TRADE_RETCODE_INVALID:
                    modify_order_failed = True
        print(f"{datetime.now()}: on_iqfeed_update: modify orders done, completiom time: {datetime.now()-iqfeed_signal_time}")
        if modify_order_failed:
            print(f"{datetime.now()}: on_iqfeed_update: modify orders failed - on_filled order will be called (this is outside)")
            self.on_filled_order(symbol, self.pose_grid[symbol], self.spread_grid[symbol],self.number_of_steps[symbol])
            print(f"{datetime.now()}: on_iqfeed_update: modify orders failed - on_filled order completed (this is outside). time since signal: {datetime.now()-iqfeed_signal_time}")
        print(f"{datetime.now()}: on_iqfeed_update: completed, total time: {datetime.now()-iqfeed_signal_time}")

# blocking function to calculate required orders. Return dictionary with keys: "buy_vol","buy_spr","sell_vol","sell_spr"
    def calc_orders(self, symbol, pose_grid, spread_grid, number_of_steps):
        print(f"{datetime.now()}: calc_orders: start")
        target_orders = pose_grid-self.pose[symbol]
        pose_index = np.searchsorted(pose_grid, self.pose[symbol], side = "left")
        # TODO: stopped here - need to apply moex_max_lots as a cap to orders volume
        if pose_index == 0:
            # no sell orders
            buy_vol = np.around(np.minimum(target_orders[pose_index+1], self.moex_max_lots[symbol]),0)
            buy_spr = spread_grid[number_of_steps-1-(pose_index+1)]
            sell_vol = None
            sell_spr = None
        elif pose_index == number_of_steps:
            #no buy orders
            sell_vol = abs(np.around(np.minimum(target_orders[pose_index-2], self.moex_max_lots[symbol]),0))
            sell_spr = spread_grid[number_of_steps-1 - (pose_index-2)]
            buy_vol = None
            buy_spr = None
        else:
            sell_vol = abs(np.around(np.minimum(target_orders[pose_index-1], self.moex_max_lots[symbol]),0))
            sell_spr = spread_grid[number_of_steps-1 - (pose_index-1)]

            if target_orders[pose_index] != 0:
                buy_vol = np.around(np.minimum(target_orders[pose_index], self.moex_max_lots[symbol]),0)
                buy_spr = spread_grid[number_of_steps-1 - pose_index]
            else:
                buy_vol = np.around(np.minimum(target_orders[pose_index+1], self.moex_max_lots[symbol]),0)
                buy_spr = spread_grid[number_of_steps-1 - (pose_index+1)]
        print(f"{datetime.now()}: calc_orders: ended")
        result ={
            "buy_vol": buy_vol,
            "buy_spr": buy_spr,
            "sell_vol": sell_vol,
            "sell_spr": sell_spr,
        }
        return result

# blocking function, that restarts orders if any one was filled
    def on_filled_order(self, symbol, pose_grid, spread_grid, number_of_steps):
        print(f"{datetime.now()}: on_filled_orders: start")
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
        #print(f"self.target_orders[symbol]['sell_ticket']: {self.target_orders[symbol]['sell_ticket']}")
        buy_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["buy_ticket"]) # returns None if no such order exists
        sell_order = self.moex_connector.load_order_ticket(self.target_orders[symbol]["sell_ticket"]) # returns None if no such order exists
        print(f"{datetime.now()}: on_filled_orders: orders loaded from MT5")
        new_moex_pose = self.moex_connector.load_position(moex_symbol)
        print(f"{datetime.now()}: on_filled_orders: new_moex_pose loaded from MT5")
        buy_indicator, sell_indicator = False, False
        buy_order_filled = 0
        sell_order_filled = 0
        if buy_order:
            if len(buy_order) == 0: # check if order is missing (filled)
                buy_order_filled = self.target_orders[symbol]["buy_vol"]
                buy_indicator = True
                self.target_orders[symbol]["buy_vol"] = None
                self.target_orders[symbol]["buy_spr"] = None
                self.target_orders[symbol]["buy_ticket"] = None
            else:
                # order is alive - updating remaining volume
                buy_order_filled = self.target_orders[symbol]["buy_vol"] - buy_order[0].volume_current
                self.target_orders[symbol]["buy_vol"] = buy_order[0].volume_current
            print(f"{datetime.now()}: on_filled_orders: buy order rountine completed")

        if sell_order:
            if len(sell_order) == 0:
                sell_order_filled = self.target_orders[symbol]["sell_vol"]
                sell_indicator = True
                self.target_orders[symbol]["sell_vol"] = None
                self.target_orders[symbol]["sell_spr"] = None
                self.target_orders[symbol]["sell_ticket"] = None
            else:
                # order is alive - updating remaining volume
                sell_order_filled = self.target_orders[symbol]["sell_vol"] - sell_order[0].volume_current
                self.target_orders[symbol]["sell_vol"] = sell_order[0].volume_current
            print(f"{datetime.now()}: on_filled_orders: sell order rountine completed")
        
        # start checking that positions are in line with orders
        print(f"{datetime.now()}: on_filled_orders: start checking positions vs orders filled")

        calc_moex_pose = self.pose[symbol] + buy_order_filled - sell_order_filled
        
        if calc_moex_pose != new_moex_pose:
            print(f"{datetime.now()}: on_filled_orders: POSITIONS INCORRECT: calc_moex_pose = {calc_moex_pose}, new_moex_pose = {new_moex_pose}")
            asyncio.create_task(self.bot.send_message(f"POSITIONS INCORRECT: calc_moex_pose = {calc_moex_pose}, new_moex_pose = {new_moex_pose}"))

        self.pose[symbol] = new_moex_pose
        print(f"{datetime.now()}: on_filled_orders: positions checking completed")
        local_target_orders = self.calc_orders(symbol, pose_grid, spread_grid, number_of_steps)

        if (local_target_orders["buy_vol"] is not None) and buy_indicator:
            buy_volume = local_target_orders["buy_vol"]
            buy_price = np.around(self.iqfeed_connector.listener.current_bid[iqfeed_symbol] + local_target_orders["buy_spr"], self.decimals[symbol])
            new_buy_order = self.moex_connector.limit_order(moex_symbol, mt5.ORDER_TYPE_BUY_LIMIT, buy_volume, buy_price)
            if new_buy_order.retcode == mt5.TRADE_RETCODE_DONE:
                self.target_orders[symbol]["buy_ticket"] = new_buy_order.order
                self.target_orders[symbol]["buy_vol"] = local_target_orders["buy_bol"]
                self.target_orders[symbol]["buy_spr"] = local_target_orders["buy_spr"]
                print(f"{datetime.now()}: on_filled_orders: buy order sent")

        if (local_target_orders["sell_vol"] is not None) and sell_indicator:
            sell_volume = local_target_orders["sell_vol"]
            sell_price = np.around(self.iqfeed_connector.listener.current_ask[iqfeed_symbol] + local_target_orders["sell_spr"], self.decimals[symbol])
            new_sell_order = self.moex_connector.limit_order(moex_symbol, mt5.ORDER_TYPE_SELL_LIMIT, sell_volume, sell_price)
            if new_sell_order.retcode == mt5.TRADE_RETCODE_DONE:
                self.target_orders[symbol]["sell_ticket"] = new_sell_order.order
                self.target_orders[symbol]["sell_vol"] = local_target_orders["sell_vol"]
                self.target_orders[symbol]["sell_spr"] = local_target_orders["sell_spr"]
                print(f"{datetime.now()}: on_filled_orders: sell order sent")
        
        print(f"{datetime.now()}: On_filled_orders: completed")
        
        pass

# order flow - limit_grid strategy. Starts with new orders, then continuously loops checking for orders
    async def order_flow(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):
        print(f"{datetime.now()}: order_flow: starting for {symbol}")
        asyncio.create_task(self.bot.send_message(f"{symbol} order_flow started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
        
        moex_orders = self.moex_connector.load_orders(moex_symbol)
        if len(moex_orders) != 0:
            print(f"{datetime.now()}: order_flow: {symbol} ERROR - Orders are not cleared before the order_flow routine")
            asyncio.create_task(self.bot.send_message(f"{symbol} ERROR - Orders are not cleared before the order_flow routine"))
            # TODO : ADD A WAY TO BREAK THE ORDER_FLOW ROUTINE!!!
            pass
        
        if (self.iqfeed_connector.listener.current_bid[iqfeed_symbol] is None) or (self.iqfeed_connector.listener.current_ask[iqfeed_symbol] is None):
            print(f"{datetime.now()}: order_flow: {symbol} ERROR - IQFeed bid/ask not loaded before the order_flow routine")
            asyncio.create_task(self.bot.send_message(f"{symbol} ERROR - IQFeed bid/ask not loaded before the order_flow routine"))
            # TODO : ADD A WAY TO BREAK THE ORDER_FLOW ROUTINE!!!
            pass

        grid = [(i-(number_of_steps - 1)/2) for i in range(number_of_steps)]
        spread_grid = np.around([spread_step * i + mid_spread for i in grid],3)
        pose_grid = np.around([pose_step * i + mid_pose for i in grid],0)
        self.pose_grid[symbol] = pose_grid
        self.spread_grid[symbol] = spread_grid
        self.number_of_steps[symbol] = number_of_steps
        
        print(f"{datetime.now()}: order_flow: {symbol} spread_grid = "+str(spread_grid)+f"\n{symbol} pose_grid = "+str(pose_grid))
        asyncio.create_task(self.bot.send_message(f"{symbol} spread_grid = "+str(spread_grid)+f"\n{symbol} pose_grid = "+str(pose_grid)))
        print(f"{datetime.now()}: order_flow: preparations are done, now sleep for 5ms")
        await asyncio.sleep(0.005)
        
        print(f"{datetime.now()}: order_flow: loading position for {symbol}")
        self.pose[symbol] = self.moex_connector.load_position(moex_symbol)
        print(f"{datetime.now()}: order_flow: starting calculation for orders for {symbol}")
        local_target_orders = self.calc_orders(symbol, pose_grid, spread_grid, number_of_steps)
        print(f"{datetime.now()}: order_flow: orders for {symbol} are calculated")
        #print(orders_to_place)
        if local_target_orders["buy_vol"] is not None:
            buy_volume = local_target_orders["buy_vol"]
            buy_price = np.around(self.iqfeed_connector.listener.current_bid[iqfeed_symbol] + local_target_orders["buy_spr"], self.decimals[symbol])
            new_buy_order = self.moex_connector.limit_order(moex_symbol, mt5.ORDER_TYPE_BUY_LIMIT, buy_volume, buy_price)
            if new_buy_order.retcode == mt5.TRADE_RETCODE_DONE:
                local_target_orders["buy_ticket"] = new_buy_order.order
            else:
                local_target_orders["buy_ticket"] = None
        else:
            local_target_orders["buy_ticket"] = None
        if local_target_orders["sell_vol"] is not None:
            sell_volume = local_target_orders["sell_vol"]
            sell_price = np.around(self.iqfeed_connector.listener.current_ask[iqfeed_symbol] + local_target_orders["sell_spr"], self.decimals[symbol])
            new_sell_order = self.moex_connector.limit_order(moex_symbol, mt5.ORDER_TYPE_SELL_LIMIT, sell_volume, sell_price)
            if new_sell_order.retcode == mt5.TRADE_RETCODE_DONE:
                local_target_orders["sell_ticket"] = new_sell_order.order
            else:
                local_target_orders["sell_ticket"] = None
        else:
            local_target_orders["sell_ticket"] = None
        print(f"{datetime.now()}: order_flow: orders for {symbol} are placed on exchange")
        self.target_orders[symbol] = local_target_orders
        print(f"{datetime.now()}: order_flow: starting the cycle, should_stop signal: {self.should_stop[symbol].is_set()}")
        
        # main loop:
        while not self.should_stop[symbol].is_set():
            self.on_filled_order(symbol, pose_grid, spread_grid, number_of_steps)
            await asyncio.sleep(self.timeout)
        
        # cleaning:
        if symbol in self.target_orders:
            del self.target_orders[symbol]
            del self.pose_grid[symbol]
            del self.spread_grid[symbol]
            del self.number_of_steps[symbol]
        print(f"{datetime.now()}: order_flow: {symbol} order_flow ended / failed")
        asyncio.create_task(self.bot.send_message(f"{symbol} order_flow ended / failed"))

# hedging loop - runs in loop continuously
    async def hedger(self, symbol):
        print(f"{datetime.now()}: hedger: starting hedger for {symbol}")
        asyncio.create_task(self.bot.send_message(f"{datetime.now()}: hedger:{symbol} started"))
        moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol 
        while not self.should_stop[symbol].is_set():
            # print(f"{datetime.now()}: start checking positions")
            timer = datetime.now()
            self.nymex_connector.load_positions([nymex_symbol])
            nymex_pose = self.nymex_connector.positions[nymex_symbol]
            moex_pose = self.moex_connector.load_position(moex_symbol)
            self.pose[symbol] = moex_pose
            print(f"{datetime.now()}: hedger: moex_pose: {moex_pose}")
            print(f"{datetime.now()}: hedger: nymex_pose: {nymex_pose}")
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
                print(f"{datetime.now()}: hedger: {symbol} starting NYMEX order execution: {trade_type} {nymex_lots_to_trade}. calcs time: {datetime.now() - timer}")
                # market order to on nymex
                nymex_trade_id = await self.nymex_connector.send_order(nymex_contract,trade_type,nymex_lots_to_trade)
                # Wait for the order to be filled
                await asyncio.sleep(0.01)
                await self.nymex_connector.order_filled_events[nymex_trade_id].wait()
                # Once the order has been filled, remove the Event
                del self.nymex_connector.order_filled_events[nymex_trade_id]
                moex_pose = self.moex_connector.load_position(moex_symbol)
                self.nymex_connector.load_positions([nymex_symbol])
                self.pose[symbol] = moex_pose
                
                #waint until IB position reflect the market order
                counter = datetime.now()
                print(f"{datetime.now()}: hedger: {symbol} nymex_pose before trade: {nymex_pose}")
                print(f"{datetime.now()}: hedger: {symbol} nymex_lots_to_trade: {nymex_lots_to_trade}")
                print(f"{datetime.now()}: hedger: {symbol} nymex loaded positions: {self.nymex_connector.positions[nymex_symbol]}")
                print((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol])
                while np.around(((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol]),0) != 0:
                    # wait until position is updated
                    print((nymex_pose - sign_open_risk*nymex_lots_to_trade) - self.nymex_connector.positions[nymex_symbol])
                    await asyncio.sleep(0.1)
                    self.nymex_connector.load_positions([nymex_symbol])
                    print(f"{datetime.now()}: hedger: waiting for {symbol} IB order execution: {(datetime.now() - counter).total_seconds()} seconds")
                    if (datetime.now() - counter) > timedelta(seconds=30):
                        print(f"{datetime.now()}: hedger: {symbol} IB order not executed in 30 seconds! Stopping trading {symbol}")
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

                print(f"{datetime.now()}: hedger: {symbol} open risk: {open_risk}. Function run time: {datetime.now() - timer}")
            await asyncio.sleep(1) # To be reduced?
        print(f"{datetime.now()}: hedger: {symbol} failed")
        asyncio.create_task(self.bot.send_message(f"{symbol} hedger failed"))

# command to start limit trading
    async def start_limit_trading(self, symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose):
        print(f"{datetime.now()}: start: starting limit grid trading:")
        nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
        nymex_symbol = nymex_contract.localSymbol
        iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
        print(f"{datetime.now()}: start: calling stop_trading {symbol}")
        self.stop_trading(symbol)
        print(f"{datetime.now()}: start: stop_trading {symbol} completed")
        if symbol == "gold1":
            await self.nymex_connector.subscribe_bid_ask(nymex_contract)
            print(f"{datetime.now()}: start: Symbol {nymex_symbol}: Current bid: {self.nymex_connector.current_bid[nymex_symbol]}, Current ask: {self.nymex_connector.current_ask[nymex_symbol]}")
            print(f"{datetime.now()}: start: subscription to NYMEX {nymex_symbol} completed")
        else:
            print(f"{datetime.now()}: start: Starting subscription from IQFeed.")
            self.iqfeed_task[symbol] = await self.iqfeed_connector.subscribe(iqfeed_symbol) # return disconnect from histConn
            print(f"{datetime.now()}: start: {symbol}: iqfeed bid: {self.iqfeed_connector.listener.current_bid[iqfeed_symbol]}, iqfeed ask: {self.iqfeed_connector.listener.current_ask[iqfeed_symbol]}")
            print(f"{datetime.now()}: start: checking iqfeed bidask data...")
            # TODO: the checker below needs to be checked for speed and necessity 
            wait_iqfeed_bidask = True
            while wait_iqfeed_bidask:
                if (self.iqfeed_connector.listener.current_bid[iqfeed_symbol] is None) or (self.iqfeed_connector.listener.current_ask[iqfeed_symbol] is None):
                    await asyncio.sleep(0.05) # wait until bid /ask loaded
                else:
                    wait_iqfeed_bidask = False
            print(f"{datetime.now()}: start: subscription to IQFeed {symbol} completed: Current bid: {self.iqfeed_bid[symbol]}, Current ask: {self.iqfeed_ask[symbol]}")
        
        if symbol in self.active_trades:
            # There is already an active task for this symbol, so we stop it before starting a new one
            print(f"{datetime.now()}: start: Stopping the active trading task for {symbol} before starting a new one.")
            self.should_stop[symbol].set()
            await asyncio.sleep(self.timeout) # not sure if needed
            await asyncio.gather(*self.active_trades[symbol]) # wait until all coroutines are completed
            print(f"{datetime.now()}: start: all coros are awaited.")
        print(f"{datetime.now()}: start: clearing stop signal:")
        self.should_stop[symbol].clear()
        print(f"{datetime.now()}: start: stop signal cleared.")

        self.active_trades[symbol] = [
            asyncio.create_task(self.order_flow(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)),
            asyncio.create_task(self.hedger(symbol)),
        ]
        if not self.scheduler_on: # check that only one scheduler is working
            asyncio.create_task(self.scheduler())
        await asyncio.gather(*self.active_trades[symbol])

# stop all trading for specific symbol or all symbols at once
    async def stop_trading(self,symbol=None):
        if symbol is None:
            # Stop all running tasks for each symbol
            print(f"{datetime.now()}: stop: stop_trading for all symbols:")
            for symbol_to_cancel in self.active_trades:
                print(f"{datetime.now()}: stop: stop_trading: {symbol_to_cancel}")
                await self.stop_symbol_trading(symbol_to_cancel)
                await asyncio.gather(*self.active_trades[symbol_to_cancel]) # wait until all coros are completed
                print(f"{datetime.now()}: stop: all coros are awaited.")
                moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol_to_cancel)]
                moex_orders = self.moex_connector.load_orders(moex_symbol)
                for order in moex_orders:
                    ticket = order.ticket
                    cancel_trade = await asyncio.to_thread(self.moex_connector.cancel_order, int(ticket))
                    if cancel_trade.retcode != mt5.TRADE_RETCODE_DONE:
                        asyncio.create_task(self.bot.send_message(f"{moex_symbol} cancel order failure! retcode: {cancel_trade.retcode}.Error code: {mt5.last_error()}"))
                try:
                    del self.active_trades[symbol_to_cancel] # TODO: check if working properly
                except:
                    print(f"{datetime.now()}: stop: del self.active_trades[symbol_to_cancel] raised exception!")
            for symbol in self.iqfeed_task:
                iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
                await asyncio.gather(*self.iqfeed_task[symbol_to_cancel]) # ensure disconnect historical_conn to IQFeed
                try:
                    del self.iqfeed_task[symbol]
                except:
                    print(f"{datetime.now()}: stop: del self.iqfeed_task[symbol] raised exception!")
        else:
            if symbol in self.active_trades:
                # Stop the task for the specified symbol
                await self.stop_symbol_trading(symbol)
                await asyncio.gather(*self.active_trades[symbol]) # wait until all coros are completed
                print(f"{datetime.now()}: stop: all coros are awaited.")
                moex_symbol = self.moex_connector.symbols[self.symbols.index(symbol)]
                moex_orders = self.moex_connector.load_orders(moex_symbol)
                for order in moex_orders:
                    ticket = order.ticket
                    cancel_trade = await asyncio.to_thread(self.moex_connector.cancel_order, int(ticket))
                    if cancel_trade.retcode != mt5.TRADE_RETCODE_DONE:
                        asyncio.create_task(self.bot.send_message(f"{moex_symbol} cancel order failure! retcode: {cancel_trade.retcode}.Error code: {mt5.last_error()}"))
                try:
                    del self.active_trades[symbol]
                except:
                    print(f"{datetime.now()}: stop: del self.active_trades[symbol_to_cancel] raised exception!")
            else:
                print(f"{datetime.now()}: stop: {symbol} is not in active_trades")
                asyncio.create_task(self.bot.send_message(f"{symbol} is not in active_trades"))
            if symbol in self.iqfeed_task:
                iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
                await asyncio.gather(*self.iqfeed_task[symbol])
                try:
                    del self.iqfeed_task[symbol]
                except:
                    print(f"{datetime.now()}: stop: del self.iqfeed_task[symbol] raised")

# raises should_stop signal and unsubscribes from nymex & IQFeed updates for selected symbol
    async def stop_symbol_trading(self, symbol):
        self.should_stop[symbol].set()
        if symbol == "gold1":
            nymex_contract = self.nymex_connector.contracts[self.symbols.index(symbol)]
            await self.nymex_connector.unsubscribe_bid_ask(nymex_contract)
            print(f"{datetime.now()}: stop_symbol_trading: NYMEX {symbol} unsubscribed")
        else:
            # IQFeed subscription?
            iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]
            self.iqfeed_connector.unsubscribe(iqfeed_symbol)
            print(f"{datetime.now()}: stop_symbol_trading: IQFEED {symbol} unsubscribed")

# time scheduler
    async def scheduler(self):
        now = datetime.now()
        stop_time = now.replace(hour = 23, minute = 47, second = 0, microsecond=0)  # 23:45 UTC
        start_time = now.replace(hour = 9, minute = 0, second = 1, microsecond=0)  # 9:01:05 UTC
        print(f"{datetime.now()}: scheduler: starting, start_time = {start_time.time()}. stop_time = {stop_time.time()}")
        asyncio.create_task(self.bot.send_message(f"starting scheduler. start_time = {start_time.time()}. stop_time = {stop_time.time()}"))
        self.scheduler_on = True
        while self.scheduler_on and self.active_trades:
            current_time = datetime.now()
            print(f"{datetime.now()}: scheduler: checking time for scheduler")
            if start_time <= current_time < stop_time:
                # TODO: add restart of the previously closed task
                print(f"{datetime.now()}: scheduler: time inside the working hours")
                pass
            else:
                # stop all trading
                print(f"{datetime.now()}: scheduler: time outside the working hours")
                self.scheduler_on = False
                await self.stop_trading()
            await asyncio.sleep(120)  # Check every 2 minutes
        print(f"{datetime.now()}: scheduler: stopped at {datetime.now().time()}")
        asyncio.create_task(self.bot.send_message(f"scheduler stopped at {datetime.now().time().replace(microsecond=0)}"))


# connections with user_interface telegram bot

    def set_bot(self, bot):
        self.bot = bot

# RUN function:
    async def run(self):
        print("mean_reversion run started")
        self.nymex_connector.ib.pendingTickersEvent  += self.on_pending_tickers
        #tasks = [asyncio.create_task(self.nymex_to_moex_signal())]
        #await asyncio.gather(*tasks)
        print("mean_reversion run ended")
        pass


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


'''
    async def iqfeed_checker(self, symbol):
        # update current bid/ask (from iqfeed)
        print(f"{datetime.now()}: starting iqfeed_checker: {symbol}")
        iqfeed_symbol = self.iqfeed_connector.symbols[self.symbols.index(symbol)]

        while not self.should_stop[symbol].is_set():
            
            iqfeed_bid_new = self.iqfeed_connector.listener.current_bid[iqfeed_symbol]
            iqfeed_ask_new = self.iqfeed_connector.listener.current_ask[iqfeed_symbol]

            if (iqfeed_bid_new != self.iqfeed_bid[symbol]) or (iqfeed_ask_new != self.iqfeed_ask[symbol]):
                self.iqfeed_bid[symbol] = iqfeed_bid_new
                self.iqfeed_ask[symbol] = iqfeed_ask_new

                print(f"{datetime.now()}: iqfeed_checker: {symbol} Current bid: {self.iqfeed_bid[symbol]}, current ask: {self.iqfeed_ask[symbol]}")
        
                moex_symbol = self.moex_connector.symbols[self.iqfeed_connector.symbols.index(iqfeed_symbol)]

            await asyncio.sleep(0.003) # TODO: ???
        print(f"{datetime.now()}: iqfeed_checker failed/ended: {symbol}")
        pass
'''