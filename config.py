from ib_insync import Future

# MOEX connection settings
MOEX_LOGIN = "your_moex_login"
MOEX_PASSWORD = "your_moex_password"
MOEX_SERVER = "your_moex_server"
MOEX_SYMBOLS = ["NG-7.23", "NG-8.23","GOLD-9.23","BR-7.23"]
MOEX_LOTS = [117,117,11,117]
MOEX_MAX_LOTS = [117,117,33,117] # max lots for limit orders

# friendly names to call strategies from Telegram:
INTERFACE_SYMBOLS = ["gas1", "gas2","gold1","brent1"]

# Number of decimal places to round to 
SYMBOL_DECIMALS = [3,3,1,2]

# NYMEX connection settings
NYMEX_API_KEY = "1"
NYMEX_HOST = "127.0.0.1"
NYMEX_PORT = 4001 # 4001 is for real account, 4002 is for simulated trading account
NYMEX_CONTRACTS = [Future(symbol = "NG",lastTradeDateOrContractMonth = "20230727",exchange = "NYMEX",localSymbol = "NGQ3",currency = "USD"),
                 Future(symbol = "NG",lastTradeDateOrContractMonth = "20230829",exchange = "NYMEX",localSymbol = "NGU3",currency = "USD"),
                 Future(symbol = "MGC",lastTradeDateOrContractMonth = "20230829",exchange = "COMEX",localSymbol = "MGCQ3",currency = "USD"),
                 Future(symbol = "BZ",lastTradeDateOrContractMonth = "20230630",exchange = "NYMEX",localSymbol = "BZQ3",currency = "USD")]
NYMEX_MAX_LOTS = [3,3,15,1]

# Database settings
DATABASE_FILE_PATH = "local_db.sqlite3"

# Telegram bot settings
TELEGRAM_API_TOKEN = ""
TELEGRAM_CHAT_IDS = []

# Web interface settings
WEB_SERVER_HOST = "localhost"
WEB_SERVER_PORT = 8000

# Logging and notification settings
LOG_LEVEL = "INFO"
LOG_FILE_PATH = "logs/application.log"

# Note: You can securely manage sensitive information using the python-decouple library.
