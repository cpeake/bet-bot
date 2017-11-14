"""Betting Bot Manager"""
import os
import logging
import threads
from sys import exit
from time import sleep
from betfair.api_ng import API

# Set up logging
logger = logging.getLogger('MAIN')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Retrieve Betfair authentication details from the environment
USERNAME = os.environ['BETFAIR_USERNAME']
PASSWORD = os.environ['BETFAIR_PASSWORD']
APP_KEY = os.environ['BETFAIR_APP_KEY']

# Retrieve live mode status from the environment
LIVE_MODE = 'LIVE_MODE' in os.environ and os.environ['LIVE_MODE'] == 'true'

if not USERNAME:
    logger.error('BETFAIR_USERNAME is not set, exiting.')
    exit()
    
if not PASSWORD:
    logger.error('BETFAIR_PASSWORD is not set, exiting.')
    exit()
    
if not APP_KEY:
    logger.error('BETFAIR_APP_KEY is not set, exiting.')
    exit()

api = API(False, ssl_prefix=USERNAME)
session_manager = threads.SessionManager(api, USERNAME, PASSWORD, APP_KEY)
session_manager.start()
sleep(5)  # Allow the session manager time to log in.

market_manager = threads.MarketManager(api)
market_book_manager = threads.MarketBookManager(api)
statistics_manager = threads.StatisticsManager(api)
account_manager = threads.AccountManager(api)
order_manager = threads.OrderManager(api)
strategy_manager = threads.StrategyManager(api, LIVE_MODE)

market_manager.start()
market_book_manager.start()
statistics_manager.start()
account_manager.start()
order_manager.start()
strategy_manager.start()
