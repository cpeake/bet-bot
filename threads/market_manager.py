import logging
import threading
import traceback
from time import time, sleep
import betbot_db

# Set up logging
logger = logging.getLogger('sss.MarketManager')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Refreshes the list of upcoming UK Horse Racing markets in the database every 15 minutes.
# If less than 60 seconds after, or prior to, a played market try again in 5 minutes.
# This avoids a race condition that could cause a market to be played twice.
class MarketManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.api = api
        self.logger = logging.getLogger('sss.MarketManager')

    def run(self):
        self.logger.info('Started Market Manager...')
        while True:
            try:
                if self.market_recently_played():
                    self.logger.info('Skipping market refresh due to last played market proximity.')
                    # Refresh in 5 minutes.
                    sleep(5 * 60)
                else:
                    self.logger.info('Refreshing the list of markets.')
                    params = {
                       'filter': {
                           'eventTypeIds': ['7'],  # horse racing
                           'marketTypeCodes': ['WIN'],
                           'marketBettingTypes': ['ODDS'],
                           'marketCountries': ['GB'],  # UK markets
                           'turnInPlayEnabled': True,  # will go in-play
                           'inPlayOnly': False  # market NOT currently in-play
                       },
                       'marketProjection': ['EVENT', 'MARKET_START_TIME'],
                       'maxResults': 1000,  # maximum allowed by Betfair
                       'sort': 'FIRST_TO_START'  # order so the next market by start time comes first
                    }
                    markets = self.api.get_markets(params)
                    if type(markets) is list:  # upsert into the DB
                        self.logger.info('Retrieved %s markets.' % len(markets))
                        for market in markets:
                            betbot_db.market_repo.upsert(market)
                    else:
                        self.logger.error('Failed to retrieve markets: resp = %s' % markets)
                    # Refresh in 15 minutes.
                    sleep(15 * 60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Market Manager Crashed: %s' % msg)
                # Wait for 1 minute before continuing.
                sleep(1 * 60)

    @staticmethod
    def market_recently_played():
        last_market = betbot_db.market_repo.get_most_recently_played()
        now = time()
        return now - last_market['marketStartTime'].timestamp() < 60  # seconds
