__version__ = 0.01

import os
import pickle
import settings
import dateutil.parser
import pytz
from logger import Logger
from time import time, sleep
from betfair.api_ng import API
from datetime import datetime, timedelta

class BetBot(object):
    def __init__(self):
        self.username = '' # set by run() function at startup
        self.logger = None # set by run() function at startup
        self.api = None # set by run() function at startup
        self.abs_path = os.path.abspath(os.path.dirname(__file__))
        self.ignores_path = '%s/ignores.pkl' % self.abs_path
        self.ignores = self.unpickle_data(self.ignores_path, []) # list of market ids
        self.betcount_path = '%s/betcount.pkl' % self.abs_path
        self.betcount = self.unpickle_data(self.betcount_path, {}) # keys = hours, vals = market ids
        self.markets = None # set by refresh_markets()
        self.throttle = {
            'next': time(), # time we can send next request. auto-updated in do_throttle()
            'wait': 1.0, # time in seconds between requests
            'keep_alive': time(), # auto-updated in keep_alive()
            'update_closed': time(), # auto-updated in update_ignores()
            'refresh_markets': time() # auto-updated in refresh_markets()
        }
        self.session = False

    def pickle_data(self, filepath = '', data = None):
        """pickle object to file"""
        f = open(filepath, 'wb')
        pickle.dump(data, f)
        f.close()

    def unpickle_data(self, filepath = '', default_object = None):
        """unpickle file to object. returns object"""
        if os.path.exists(filepath):
            f = open(filepath, 'rb')
            data = pickle.load(f)
            f.close()
            return data
        return default_object # return default object (empty)

    def do_throttle(self):
        """return when it's safe to continue"""
        now = time()
        if now < self.throttle['next']:
            wait = self.throttle['next'] - now
            sleep(wait)
        self.throttle['next'] = time() + self.throttle['wait']
        return

    def do_login(self, username = '', password = ''):
        """login to betfair & set session status"""
        self.session = False
        resp = self.api.login(username, password)
        if resp == 'SUCCESS':
            self.session = True
        else:
            self.session = False # failed login
            msg = 'api.login() resp = %s' % resp
            raise Exception(msg)

    def keep_alive(self):
        """refresh login session. sessions expire after 20 mins.
        NOTE: betfair throttle = 1 req every 7 mins
        """
        now = time()
        if now > self.throttle['keep_alive']:
            # refresh
            self.session = False
            resp = self.api.keep_alive()
            if resp == 'SUCCESS':
                self.throttle['keep_alive'] = now + (15 * 60) # add 15 mins
                self.session = True
            else:
                self.session = False
                msg = 'api.keep_alive() resp = %s' % resp
                raise Exception(msg)

    def get_markets(self):
        """returns a list of UK horse racing WIN markets"""
        params = {
            'filter': {
                'eventTypeIds': ['7'],
                'marketTypeCodes': ['WIN'],
                'marketBettingTypes': ['ODDS'],
                'marketCountries': ['GB'],
                'turnInPlayEnabled': True, # will go in-play
                'inPlayOnly': False # market NOT currently in-play
            },
            'marketProjection': ['EVENT','RUNNER_DESCRIPTION','MARKET_START_TIME'],
            'maxResults': 1000, # maximum allowed by betfair
            'sort': 'FIRST_TO_START'
        }
        # send the request
        markets = self.api.get_markets(params)
        if type(markets) is list:
            msg = 'Found %s markets...' % len(markets)
            self.logger.xprint(msg)
            return markets
        else:
            msg = 'api.get_markets() resp = %s' % markets
            raise Exception(msg)

    def refresh_markets(self):
        """refresh available markets every 15 minutes"""
        now = time()
        if now > self.throttle['refresh_markets']:
            markets = self.get_markets()
            for market_id in self.ignores: # remove markets already bet on
                self.markets.remove(market_id)
            self.markets = markets
            self.throttle['refresh_markets'] = now + (15 * 60) # add 15 mins

    def get_market_book(self, market = None):
        books = self.api.get_market_books([market['marketId']])
        self.logger.xprint(books)
        if type(books) is list:
            return books[0]
        else:
            msg = 'api.get_market_books() resp = %s' % books
            raise Exception(msg)

    def get_favourite(self, market_book = None):
        favourite = None
        bestPrice = float("inf")
        for runner in market_book['runners']:
            if runner['status'] == 'ACTIVE':
                if 'lastPriceTraded' in runner: # sometimes there isn't a lastPriceTraded available
                    if runner['lastPriceTraded'] < bestPrice:
                        favourite = runner
                        bestPrice = runner['lastPriceTraded']
        self.logger.xprint('Favourite runner identified: %s' % favourite)
        return favourite

    def create_bets(self, market = None):
        """place bets on market
        @market: type = dict returned from get_markets()
        NOTE: restricted to placing bets on a single market
        """
        market_bets = {}
        if market:
            market_id = market['marketId']
            book = self.get_market_book(market)
            runner = self.get_favourite(book)
            market_bets[market_id] = {'bets': []}
            new_bet = {}
            new_bet['selectionId'] = runner['selectionId']
            new_bet['side'] = 'BACK' # or could be LAY here
            # new_bet['orderType'] = 'LIMIT'
            # new_bet['limitOrder'] = {
            #    'size': 2.0,
            #    'price': 1.01,
            #    'persistenceType': 'LAPSE' # LAPSE at in-play. Set as 'PERSIST' to retain in-play.
            #}
            new_bet['orderType'] = 'MARKET_ON_CLOSE'
            new_bet['marketOnCloseOrder'] = {
                'liability': 2.0
            }
            market_bets[market_id]['bets'].append(new_bet)
        return market_bets

    def update_ignores(self, market_id = ''):
        """update ignores list"""
        if market_id:
            # add market to ignores dict
            if market_id not in self.ignores:
                self.ignores.append(market_id)
                self.markets.remove(market_id)
                self.pickle_data(self.ignores_path, self.ignores)
        else:
            # check for closed markets (once every 2 hours)
            count = len(self.ignores)
            now = time()
            if count > 0 and now > self.throttle['update_closed']:
                secs = 2 * 60 # 2 minutes
                self.throttle['update_closed'] = now + secs
                msg = 'Checking %s markets for closed status.' % count
                self.logger.xprint(msg)
                for i in range(0, count, 5):
                    market_ids = self.ignores[i:i+5] # list of upto 5 market ids
                    self.do_throttle()
                    books = self.get_market_books(market_ids)
                    for book in books:
                        if book['status'] == 'CLOSED':
                            # remove from ignores
                            self.ignores.remove(book['marketId'])
                            self.pickle_data(self.ignores_path, self.ignores)
                            # get settled bets
                            self.logger.xprint("Get settled bets")

    def place_bets(self, market_bets = None):
        """loop through markets and place bets
        @market_bets: type = dict returned from create_bets()
        """
        for market_id in market_bets:
            bets = market_bets[market_id]['bets']
            if bets:
                resp = self.api.place_bets(market_id, bets)
                if (type(resp) is dict
                    and 'status' in resp
                    ):
                    if resp['status'] == 'SUCCESS':
                        # add to ignores
                        self.update_ignores(market_id)
                        msg = 'PLACE BETS: SUCCESS'
                        self.logger.xprint(msg)
                    else:
                        if resp['errorCode'] == 'INSUFFICIENT_FUNDS':
                            msg = 'PLACE BETS: FAIL (%s)' % resp['errorCode']
                            self.logger.xprint(msg)
                            sleep(180) # wait 3 minutes
                        else:
                            msg = 'PLACE BETS: FAIL (%s)' % resp['errorCode']
                            self.logger.xprint(msg, True) # do not raise error - allow bot to continue
                            # add to ignores
                            self.update_ignores(market_id)
                else:
                    msg = 'PLACE BETS: FAIL\n%s' % resp
                    raise Exception(msg)

    def run(self, username = '', password = '', app_key = '', aus = False):
        # create the API object
        self.username = username
        self.api = API(aus, ssl_prefix = username)
        self.api.app_key = app_key
        self.logger = Logger(aus)
        self.logger.bot_version = __version__
        # login to betfair api-ng
        self.do_login(username, password)
        while self.session:
            self.do_throttle()
            self.keep_alive() # refresh login session (every 15 mins)
            self.refresh_markets() # refresh available markets (every 15 mins), ordered first to last start time
            if self.markets:
                next_market = self.markets[0]
                name = next_market['marketName']
                venue = next_market['event']['venue']
                start_time = dateutil.parser.parse(next_market['marketStartTime'])
                # self.logger.xprint(next_market)
                msg = "Next market is the %s %s at %s." % (venue, name, start_time)
                self.logger.xprint(msg)
                now = datetime.now(pytz.utc)
                wait = (start_time - now).total_seconds()
                # if wait < 60: # process the next market, less than a minute to go before start
                if wait < float('Inf'): # use this for testing purposes, causes bets to be created immediately
                    msg = 'Generating bets for %s %s, starting in <1 minute.' % (venue, name)
                    self.logger.xprint(msg)
                    market_bets = self.create_bets(next_market)
                    self.logger.xprint(market_bets)
                    if market_bets:
                        self.place_bets(market_bets)
                else: # wait until a minute before the next market is due to start
                    mins, secs = divmod(wait, 60)
                    msg = "Sleeping until 1 minute before the next market starts."
                    self.logger.xprint(msg)
                    time_target = time() + wait - 60
                    while time() < time_target:
                        self.keep_alive() # refresh login session (runs every 15 mins)
                        sleep(0.5) # CPU saver!
            sleep(5)
        if not self.session:
            msg = 'SESSION TIMEOUT'
            raise Exception(msg)
