import aiosqlite
import asyncio
import pandas as pd
from typing import List, Dict, Optional
from config import DATABASE_FILE_PATH

class DatabaseHandler:
    def __init__(self, db_path: str = DATABASE_FILE_PATH):
        self.db_path = db_path
        self._init_db_task = asyncio.create_task(self.init_db())

# initializing databases. TODO: (1) COLUMN NAMES (2) calendar? (3) archives (4) convertion to CSV
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trade_parameters (
                    name TEXT PRIMARY KEY,
                    value REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_grid_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    moex_symbol TEXT,
                    nymex_symbol TEXT,
                    trade_type TEXT,
                    trade_size INTEGER,
                    moex_price REAL,
                    nymex_price REAL,
                    execution_time REAL,
                    fees REAL
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS limit_grid_moex_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    moex_symbol TEXT,
                    nymex_symbol TEXT,
                    trade_type TEXT,
                    trade_size INTEGER,
                    moex_price REAL,
                    nymex_price REAL,
                    execution_time REAL,
                    fees REAL
                )
            """)            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS limit_grid_nymex_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    moex_symbol TEXT,
                    nymex_symbol TEXT,
                    trade_type TEXT,
                    trade_size INTEGER,
                    moex_price REAL,
                    nymex_price REAL,
                    execution_time REAL,
                    fees REAL
                )
            """)                 
            await db.commit()

    async def get_trade_parameters(self) -> Dict[str, float]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT name, value FROM trade_parameters")
            rows = await cursor.fetchall()
            return {name: value for name, value in rows}

    async def update_trade_parameter(self, name: str, value: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO trade_parameters (name, value) VALUES (?, ?)", (name, value))
            await db.commit()

# storing the executed trades from a DataFrame

# market grid strategy:
    async def store_market_grid_trade(self, trade_data: pd.DataFrame):
        loop = asyncio.get_running_loop() # use the loop, that is created by asyncio.run
        await loop.run_in_executor(None, self._store_executed_trade_blocking, trade_data)

    def _store_market_grid_trade_blocking(self, trade_data: pd.DataFrame):
        with aiosqlite.connect(self.db_path) as conn:
            trade_data.to_sql('market_grid_trades', conn, if_exists='append', index=False)

# limit grid strategy, moex trades:
    async def store_limit_grid_moex_trade(self, trade_data: pd.DataFrame):
        loop = asyncio.get_running_loop() # use the loop, that is created by asyncio.run
        await loop.run_in_executor(None, self._store_executed_trade_blocking, trade_data)

    def _store_limit_grid_moex_trade_blocking(self, trade_data: pd.DataFrame):
        with aiosqlite.connect(self.db_path) as conn:
            trade_data.to_sql('limit_grid_moex_trades', conn, if_exists='append', index=False)

# limit grid strategy, NYMEX trades:
    async def store_limit_grid_nymex_trade(self, trade_data: pd.DataFrame):
        loop = asyncio.get_running_loop() # use the loop, that is created by asyncio.run
        await loop.run_in_executor(None, self._store_executed_trade_blocking, trade_data)

    def _store_limit_grid_nymex_trade_blocking(self, trade_data: pd.DataFrame):
        with aiosqlite.connect(self.db_path) as conn:
            trade_data.to_sql('limit_grid_nymex_trades', conn, if_exists='append', index=False)

# not used:
    async def get_market_grid_trades(self) -> List[Dict[str, any]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT id, timestamp, moex_symbol, nymex_symbol, trade_type, trade_size,
                       moex_price, nymex_price, execution_time, fees
                FROM market_grid_trades
                ORDER BY timestamp DESC
            """)
            rows = await cursor.fetchall()
            return [{
                "id": row[0], "timestamp": row[1], "moex_symbol": row[2], "nymex_symbol": row[3],
                "trade_type": row[4], "trade_size": row[5], "moex_price": row[6], "nymex_price": row[7],
                "execution_time": row[8], "fees": row[9]
            } for row in rows]


    async def wait_until_ready(self):
        """Wait until the database is ready to be used."""
        await self._init_db_task