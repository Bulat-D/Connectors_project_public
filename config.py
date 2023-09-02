from ib_insync import Future

# MOEX connection settings
MOEX_LOGIN = "your_moex_login"
MOEX_PASSWORD = "your_moex_password"
MOEX_SERVER = "your_moex_server"
MOEX_SYMBOLS = ["NG-9.23", "NG-10.23","GOLD-9.23","BR-10.23"]
MOEX_LOTS = [117,117,11,117]
MOEX_MAX_LOTS = [117,117,33,117] # max lots for limit orders

# friendly names to call strategies from Telegram:
INTERFACE_SYMBOLS = ["gas1", "gas2","gold1","brent1"]

# IQFeed symbols:
IQFEED_SYMBOLS = ["QNGV23", "QNGX23", "QMGCZ23", "QBZX23"] # COMEX / GOLD / QMGCQ23 - no subscription. IB data should be used

# Number of decimal places to round to 
SYMBOL_DECIMALS = [3,3,1,2]

# NYMEX connection settings
NYMEX_API_KEY = "1"
NYMEX_HOST = "127.0.0.1"
NYMEX_PORT = 4001 # 4001 is for real account, 4002 is for simulated trading account
NYMEX_CONTRACTS = [Future(symbol = "NG",lastTradeDateOrContractMonth = "20230927",exchange = "NYMEX",localSymbol = "NGV3",currency = "USD"),
                 Future(symbol = "NG",lastTradeDateOrContractMonth = "20231027",exchange = "NYMEX",localSymbol = "NGX3",currency = "USD"),
                 Future(symbol = "MGC",lastTradeDateOrContractMonth = "20231227",exchange = "COMEX",localSymbol = "MGCZ3",currency = "USD"),
                 Future(symbol = "BZ",lastTradeDateOrContractMonth = "20230831",exchange = "NYMEX",localSymbol = "BZX3",currency = "USD")]
NYMEX_MAX_LOTS = [1,1,5,1]

# Database settings
DATABASE_FILE_PATH = "local_db.sqlite3"

# Telegram bot settings
TELEGRAM_API_TOKEN = ""
TELEGRAM_CHAT_IDS = [240602279]

# Web interface settings
WEB_SERVER_HOST = "localhost"
WEB_SERVER_PORT = 8000

# Logging and notification settings
LOG_LEVEL = "INFO"
LOG_FILE_PATH = "logs/application.log"

# Note: You can securely manage sensitive information using the python-decouple library.