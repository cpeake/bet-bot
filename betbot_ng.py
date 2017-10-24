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
from pymongo import MongoClient

class BetBot(object):
    def __init__(self):
        self.username = '' # set by run() function at startup
        self.logger = None # set by run() function at startup
        self.api = None # set by run() function at startup
        self.markets = None # set by refresh_markets()
        self.throttle = {
            'next': time(), # time we can send next request. auto-updated in do_throttle()
            'wait': 1.0, # time in seconds between requests
            'keep_alive': time(), # auto-updated in keep_alive()
            'update_closed': time(), # auto-updated in update_ignores()
            'refresh_markets': time() # auto-updated in refresh_markets()
        }
        self.session = False

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

    def upsert_market(self, market = None):
        if market:
            key = {'marketId': market['marketId']}
            data = market
            self.db.markets.update(key, data, upsert = True)

    def upsert_market_book(self, market_book = None):
        if market_book:
            key = {'marketId': market_book['marketId']}
            data = market_book
            self.db.market_books.update(key, data, upsert = True)

    def refresh_markets(self):
        """refresh available markets every 15 minutes"""
        now = time()
        if now > self.throttle['refresh_markets']:
            msg = 'Refreshing list of markets.'
            self.logger.xprint(msg)
            # define the filter
            params = {
                'filter': {
                    'eventTypeIds': ['7'], # horse racing
                    'marketTypeCodes': ['WIN'],
                    'marketBettingTypes': ['ODDS'],
                    'marketCountries': ['GB'], # UK races
                    'turnInPlayEnabled': True, # will go in-play
                    'inPlayOnly': False # market NOT currently in-play
                },
                'marketProjection': ['EVENT','RUNNER_DESCRIPTION','MARKET_START_TIME'],
                'maxResults': 1000, # maximum allowed by betfair
                'sort': 'FIRST_TO_START' # order so the next race by start time comes first
            }
            # send the request
            markets = self.api.get_markets(params)
            if type(markets) is list: # upsert into the DB
                msg = 'Retrieved %s markets.' % len(markets)
                self.logger.xprint(msg)
                for market in markets:
                    self.upsert_market(market)
            else:
                msg = 'Failed to retrieve markets: resp = %s' % markets
                raise Exception(msg)
            # update throttle to refresh again in 15 minutes
            self.throttle['refresh_markets'] = now + (15 * 60) # add 15 mins

    def set_market_played(self, market = None):
        """set the provided market as played (i.e. bets have been placed successfully)"""
        if market:
            market['played'] = True
            self.upsert_market(market)
            
    def set_market_skipped(self, market = None, errorCode = ''):
        """set the provided market as skipped (i.e. bets have not been placed successfully)
           either because the market was in the past when betting was attempted, or
           an attempt to place bets failed
        """
        if market:
            market['played'] = False
            if errorCode:
                market['errorCode'] = errorCode
            self.upsert_market(market)
        
    def next_playable_market(self):
        """returns the next playable market"""
        market = None
        self.logger.xprint("Finding next playable market")
        cursor = self.db.markets.find({
            "played": { "$exists": 0 }
        }).sort([('marketStartTime', 1)]).limit(1)
        if cursor[0]:
            market = cursor[0]
        self.logger.xprint(market)
        return market
        
    def get_market_by_id(self, market_id = ''):
        msg = 'Finding market by ID: %s' % market_id
        self.logger.xprint(msg)
        cursor = self.db.markets.find_one({
            "marketId": market_id
        })
        if cursor[0]:
            return cursor[0]
        else:
            return None

    def get_market_book(self, market = None):
        books = self.api.get_market_books([market['marketId']])
        self.logger.xprint(books)
        if type(books) is list:
            return books[0]
        else:
            msg = 'Failed to get market book: resp = %s' % books
            raise Exception(msg)

    def get_favourite(self, market_book = None):
        favourite = None
        bestPrice = float("inf")
        for runner in market_book['runners']:
            if runner['status'] == 'ACTIVE': # the horse has got to be running still!
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
            new_bet['orderType'] = 'MARKET_ON_CLOSE'
            new_bet['marketOnCloseOrder'] = {
                'liability': 2.0
            }
            market_bets[market_id]['bets'].append(new_bet)
        return market_bets

    def place_bets(self, market = None, market_bets = None):
        """loop through markets and place bets
        @market_bets: type = dict returned from create_bets()
        """
        for market_id in market_bets:
            bets = market_bets[market_id]['bets']
            venue = market['event']['venue']
            name = market['marketName']
            if bets:
                resp = self.api.place_bets(market_id, bets)
                if (type(resp) is dict
                    and 'status' in resp
                    ):
                    if resp['status'] == 'SUCCESS':
                        # set the market as played
                        self.set_market_played(market)
                        msg = 'Successfully placed bet(s) on %s %s.' % (venue, name)
                        self.logger.xprint(msg)
                    else:
                        msg = 'Failed to place bet(s) on %s %s. (Error: %s)' % (venue, name, resp['errorCode'])
                        self.logger.xprint(msg, True) # do not raise error - allow bot to continue
                        # set the market as skipped, it's too late to try again
                        self.set_market_skipped(market, resp['errorCode'])
                else:
                    msg = 'Failed to place bet(s) on %s %s - resp = %s' % (venue, name, resp)
                    raise Exception(msg)

    def run(self, username = '', password = '', app_key = '', aus = False):
        # create the API object
        self.username = username
        self.api = API(aus, ssl_prefix = username)
        self.api.app_key = app_key
        self.logger = Logger(aus)
        self.logger.bot_version = __version__
        # connect to MongoDB
        client = MongoClient()
        self.db = client.betbot
        self.logger.xprint(self.db)
        # login to betfair api-ng
        self.do_login(username, password)
        while self.session:
            self.do_throttle()
            self.keep_alive() # refresh login session (every 15 mins)
            self.refresh_markets() # refresh available markets (every 15 mins)
            next_market = self.next_playable_market()
            if next_market:
                name = next_market['marketName']
                venue = next_market['event']['venue']
                start_time = dateutil.parser.parse(next_market['marketStartTime'])
                msg = "Next market is the %s %s at %s." % (venue, name, start_time)
                self.logger.xprint(msg)
                now = datetime.now(pytz.utc)
                wait = (start_time - now).total_seconds()
                if wait < 0: # "next" market is in the past and can't be played
                    msg = "Market start is in the past, marking as not played and moving on."
                    self.logger.xprint(msg)
                    self.set_market_skipped(next_market, 'MARKET_IN_PAST')
                else:
                    # if wait < 60: # process the next market, less than a minute to go before start
                    if wait < float('Inf'): # use this for testing purposes, causes bets to be created immediately
                        msg = 'Generating bets for %s %s, starting in <1 minute.' % (venue, name)
                        self.logger.xprint(msg)
                        market_bets = self.create_bets(next_market)
                        self.logger.xprint(market_bets)
                        if market_bets:
                            self.place_bets(next_market, market_bets)
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
