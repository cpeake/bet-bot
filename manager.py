"""Betfair Bot Manager"""
from time import sleep
import os
import traceback
from sys import argv, exit

### USER INFO ###
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

EXIT_ON_ERROR = True # set to False when bot is ready to run 24/7

while True: # loop forever
    try:
        from betbot_ng import BetBot
        from logger import Logger
        log = Logger()
        log.xprint('Starting Betting Bot')
        # start bot
        bot = BetBot()
        bot.run(USERNAME, PASSWORD, APP_KEY)
    except Exception as exc:
        from logger import Logger
        log = Logger()
        msg = traceback.format_exc()
        http_err = 'ConnectionError:'
        if http_err in msg:
            msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
        msg = 'Bot Crashed: %s' % msg
        log.xprint(msg, err = True)
        if EXIT_ON_ERROR: exit()
    sleep(60) # wait for betfair errors to clear
