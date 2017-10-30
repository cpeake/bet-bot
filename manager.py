"""Betfair Bot Manager"""
from time import sleep
import os
import logging
import traceback
from sys import exit, argv

# Set up logging
# TODO: consider logging changes for running on Heroku
logger = logging.getLogger('betbot_application')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Retrieve Betfair authentication details from the environment
USERNAME = os.environ['BETFAIR_USERNAME']
PASSWORD = os.environ['BETFAIR_PASSWORD']
APP_KEY = os.environ['BETFAIR_APP_KEY']

if not USERNAME:
    print('BETFAIR_USERNAME is not set, exiting.')
    exit()
    
if not PASSWORD:
    print('BETFAIR_PASSWORD is not set, exiting.')
    exit()
    
if not APP_KEY:
    print('BETFAIR_APP_KEY is not set, exiting.')
    exit()

EXIT_ON_ERROR = True  # set to False when bot is ready to run 24/7

# Are we just simulating bets?
SIM = False
if '--simulate' in argv:
    SIM = True

while True:  # loop forever
    try:
        # Start BetBot
        from betbot_ng import BetBot
        logger.info('Starting BetBot')
        bot = BetBot()
        bot.run(USERNAME, PASSWORD, APP_KEY, SIM)
    except Exception as exc:
        msg = traceback.format_exc()
        http_err = 'ConnectionError:'
        if http_err in msg:
            msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
        msg = 'BetBot Crashed: %s' % msg
        logger.error(msg)
        if EXIT_ON_ERROR:
            exit()
    sleep(60)  # wait for Betfair errors to clear
