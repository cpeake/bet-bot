__version__ = 0.01

import os
import pickle
import settings
import dateutil.parser
import pytz
import pymongo
from logger import Logger
from time import time, sleep
from betfair.api_ng import API
from datetime import datetime, timedelta
from pymongo import MongoClient
from slackclient import SlackClient

class BetBot(object):
    def __init__(self):
        self.username = '' # set by run() function at startup
        self.logger = None # set by run() function at startup
        self.api = None # set by run() function at startup
        self.throttle = {
            'next': time(), # time we can send next request. auto-updated in do_throttle()
            'wait': 1.0, # time in seconds between requests
            'keep_alive': time(), # auto-updated in keep_alive()
            'update_orders': time(), # auto-updated in update_orders()
            'refresh_markets': time() # auto-updated in refresh_markets()
        }
        self.session = False

    def get_stake_by_ladder_position(self, position = 0):
        return settings.stake_ladder[position] * minimum_stake * settings.stake_multiplier

    def get_lay_liability(self, stake = 0.0, price = 0.0):
        """get the lay liability based on the provided stake and price"""
        return stake * (price - 1.0)

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
            # convert datetime strings to proper datetimes for storing as ISODate
            marketStartTime = market['marketStartTime']
            openDate = market['event']['openDate']
            if marketStartTime and type(marketStartTime) is str:
                market['marketStartTime'] = dateutil.parser.parse(marketStartTime)
            if openDate and type(openDate) is str:
                market['event']['openDate'] = dateutil.parser.parse(openDate)
            key = {'marketId': market['marketId']}
            self.db.markets.update(key, market, upsert = True)

    def insert_market_book(self, market_book = None):
        if market_book:
            # convert datetime strings to proper datetimes for storing as ISODate
            lastMatchTime = market_book['lastMatchTime']
            if lastMatchTime and type(lastMatchTime) is str:
                market_book['lastMatchTime'] = dateutil.parser.parse(lastMatchTime)
            self.db.market_books.insert_one(market_book)
               
    def get_instruction(self, bet_id = ''):
        if bet_id:
            return self.db.instructions.find_one({'betId': bet_id})
        return None
        
    def insert_instructions(self, market = None, instructions = []):
        if market:
            market_id = market['marketId']
            for instruction in instructions:
                # convert datetime string to proper datetime for storing as ISODate
                placedDate = instruction['placedDate']
                if placedDate and type(placedDate) is str:
                    instruction['placedDate'] = dateutil.parser.parse(placedDate)
                instruction['marketId'] = market_id
                instruction['settled'] = False
                self.db.instructions.insert_one(instruction)
          
    def upsert_instruction(self, instruction = None):
        if instruction:
            # convert datetime string to proper datetime for storing as ISODate
            placedDate = instruction['placedDate']
            if placedDate and type(placedDate) is str:
                instruction['placedDate'] = dateutil.parser.parse(placedDate)
            key = {'betId': instruction['betId']}
            self.db.instructions.update(key, instruction, upsert = True)
                
    def upsert_orders(self, orders = []):
        for order in orders:
            # convert date strings to datetimes (ISODates in MongoDB)
            placedDate = order['placedDate']
            marketStartTime = order['itemDescription']['marketStartTime']
            settledDate = order['settledDate']
            lastMatchedDate = order['lastMatchedDate']
            if placedDate and type(placedDate) is str:
                order['placedDate'] = dateutil.parser.parse(placedDate)
            if marketStartTime and type(placedDate) is str:
                order['itemDescription']['marketStartTime'] = dateutil.parser.parse(marketStartTime)
            if settledDate and type(settledDate) is str:
                order['settledDate'] = dateutil.parser.parse(settledDate)
            if lastMatchedDate and type(lastMatchedDate) is str:
                order['lastMatchedDate'] = dateutil.parser.parse(lastMatchedDate)
            key = {'betId': order['betId']}
            self.db.orders.update(key, order, upsert = True)

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
                    'marketCountries': ['GB'], # UK markets
                    'turnInPlayEnabled': True, # will go in-play
                    'inPlayOnly': False # market NOT currently in-play
                },
                'marketProjection': ['EVENT','MARKET_START_TIME'],
                'maxResults': 1000, # maximum allowed by betfair
                'sort': 'FIRST_TO_START' # order so the next market by start time comes first
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

    def get_active_instructions(self):
        """returns instructions not marked as settled"""
        instructions = []
        for instruction in self.db.instructions.find({"settled": False}):
            instructions.append(instruction)
        return instructions

    def set_instructions_settled(self, cleared_orders = []):
        for order in cleared_orders:
            bet_id = order['betId']
            msg = 'Marking instruction %s as settled.' % bet_id
            self.logger.xprint(msg)
            instruction = self.get_instruction(bet_id)
            if instruction:
                instruction['settled'] = True
                self.upsert_instruction(instruction)
            else:
                msg = 'Instruction with betId %s not found.' & bet_id
                self.logger.xprint(msg)

    def update_orders(self):
        now = time()
        if now > self.throttle['update_orders']:
            active_instructions = self.get_active_instructions()
            if active_instructions:
                msg = 'Updating order(s) on %s active instruction(s).' % len(active_instructions)
                self.logger.xprint(msg)
                bet_ids = []
                for bet in active_instructions:
                    bet_ids.append(bet['betId'])
                current_orders = self.api.get_current_orders(bet_ids)
                self.upsert_orders(current_orders)
                cleared_orders = self.api.get_cleared_orders(bet_ids)
                self.upsert_orders(cleared_orders)
                self.set_instructions_settled(cleared_orders)
            # update throttle to refresh again in 1 minutes
            self.throttle['update_orders'] = now + 60 # add 1 min

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
        cursor = self.db.markets.find({
            "played": { "$exists": 0 }
        }).sort([('marketStartTime', 1)]).limit(1)
        if cursor[0]:
            market = cursor[0]
        return market
        
    def get_market_by_id(self, market_id = ''):
        msg = 'Retrieving market with ID %s' % market_id
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
        return favourite

    def strategy_won_yesterday(self, strategy_ref = ''):
        # Find all cleared orders (profit attribute exists) for this strategy from yesterday and sum the profit.
        # if sum(profit) < 0 then False otherwise True (profit >= 0 or no cleared orders)
        now = datetime.utcnow()
        today_sod = datetime(now.year, now.month, now.day, 0, 0)
        yesterday_sod = today_sod - timedelta(days=1)
        orders = db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": "ABS1",
            "settledDate": {'$gte':yesterday_sod,'$lt':today_sod}
        })
        if orders.count() == 0:
            return True # if there are no orders default to True
        profit = 0.0
        for order in orders:
            profit += order['profit']
        return profit >= 0.0
        
    def strategy_won_last_market(self, strategy_ref = ''):
        """return True if the most recent bet made by the strategy WON
           or if there is no previous bet made by the strategy, False otherwise"""
        orders = self.db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": "ABS1"
        }).sort('settledDate', pymongo.DESCENDING).limit(1)
        if orders[0]:
            order = orders[0]
            if order['betOutcome'] == 'WON':
                return True
            else:
                return False
        else:
            return True
            
    def strategy_won_last_market_today(self, strategy_ref = ''):
        """return True if the most recent bet made by the strategy WON
           or if there is no previous bet made by the strategy, False otherwise"""
        now = datetime.utcnow()
        today_sod = datetime(now.year, now.month, now.day, 0, 0)
        today_eod = datetime(now.year, now.month, now.day, 23, 59)
        orders = self.db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": "ABS1",
            "settledDate": {'$gte':today_sod,'$lt':today_eod}
        }).sort('settledDate', pymongo.DESCENDING).limit(1)
        if orders[0]:
            order = orders[0]
            if order['betOutcome'] == 'WON':
                return True
            else:
                return False
        else:
            return True

    def create_lay_all_bets(self, market = None):
        strategy_bets = []
        #if market:
        # if this is the first day of trading or the strategy generated a profit on the previous day,
        # place a lay bet at minimum liability in the Fibonacci sequence, else place a lay bet with
        # liability at the next number in the Fibonnaci sequence
        if market:
            book = self.get_market_book(market)
            self.insert_market_book(book)
            runner = self.get_favourite(book)
            stake = 2.0
            new_bet = {}
            new_bet['selectionId'] = runner['selectionId']
            new_bet['handicap'] = 0
            new_bet['side'] = 'LAY' # or could be LAY here
            new_bet['orderType'] = 'MARKET_ON_CLOSE'
            new_bet['marketOnCloseOrder'] = {
                'liability': self.get_lay_liability(stake, runner['lastPriceTraded'])
            }
            strategy_bets.append(new_bet)
        return strategy_bets
        
    def create_bet_all_bets(self, market = None):
        strategy_bets = []
        if market:
            book = self.get_market_book(market)
            self.insert_market_book(book)
            runner = self.get_favourite(book)
            stake = 2.0
            new_bet = {}
            new_bet['selectionId'] = runner['selectionId']
            new_bet['handicap'] = 0
            new_bet['side'] = 'BACK' # or could be LAY here
            new_bet['orderType'] = 'MARKET_ON_CLOSE'
            new_bet['marketOnCloseOrder'] = {
                'liability': stake
            }
            strategy_bets.append(new_bet)
        return strategy_bets

    def place_bets(self, market = None, strategy_bets = None):
        """place bets for all strategies on a given market"""
        venue = market['event']['venue']
        name = market['marketName']
        if strategy_bets:
            for strategy_ref in strategy_bets:
                market_bets = strategy_bets[strategy_ref]
                self.logger.xprint(market_bets)
                resp = self.api.place_bets(market['marketId'], market_bets, strategy_ref)
                if (type(resp) is dict
                    and 'status' in resp
                    ):
                    if resp['status'] == 'SUCCESS':
                        # set the market as played
                        self.set_market_played(market)
                        # persist the bet execution reports in the DB
                        self.insert_instructions(market, resp['instructionReports'])
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

    def post_slack_message(self, msg = ''):
        if msg:
            self.sc.api_call(
                "chat.postMessage",
                channel="#dev-bot",
                text=msg
        )

    def run(self, username = '', password = '', app_key = ''):
        # create the API object
        self.username = username
        self.api = API(False, ssl_prefix = username) # connect to the UK (rather than AUS) API
        self.api.app_key = app_key
        self.logger = Logger()
        self.logger.bot_version = __version__
        # connect to MongoDB
        client = MongoClient()
        self.db = client.betbot
        self.logger.xprint(self.db)
        # login to Betfair api-ng
        self.do_login(username, password)
        # login to Slack        
        self.sc = SlackClient(os.environ["SLACK_API_TOKEN"])
        self.post_slack_message('BetBot has started! :tada:')
        while self.session:
            self.do_throttle()
            self.keep_alive() # refresh login session (every 15 mins)
            self.refresh_markets() # refresh available markets (every 15 mins)
            self.update_orders() # update current and cleared orders (every 1 minute)
            next_market = self.next_playable_market()
            if next_market:
                name = next_market['marketName']
                venue = next_market['event']['venue']
                start_time = next_market['marketStartTime']
                msg = "Next market is the %s %s at %s." % (venue, name, start_time)
                self.logger.xprint(msg)
                now = datetime.utcnow()
                wait = (start_time - now).total_seconds()
                if wait < 0: # "next" market is in the past and can't be played
                    msg = "%s %s has already started, skipping." % (venue, name)
                    self.logger.xprint(msg)
                    self.set_market_skipped(next_market, 'MARKET_IN_PAST')
                else:
                    if wait < 60: # process the next market, less than a minute to go before start
                        #if wait < float('Inf'): # use this for testing purposes, causes bets to be created immediately
                        strategy_bets = {}
                        # strategy_bets['ALS1'] = self.create_lay_all_bets(next_market)
                        strategy_bets['ABS1'] = self.create_bet_all_bets(next_market)
                        msg = 'Generated bets on %s %s.\n%s' % (venue, name, strategy_bets)
                        self.logger.xprint(msg)
                        if strategy_bets:
                            self.place_bets(next_market, strategy_bets)
                    else: # wait until a minute before the next market is due to start
                        mins, secs = divmod(wait, 60)
                        msg = "Sleeping until 1 minute before %s %s starts." % (venue, name)
                        self.logger.xprint(msg)
                        time_target = time() + wait - 60
                        while time() < time_target:
                            self.keep_alive() # refresh login session (runs every 15 mins)
                            self.update_orders() # update current and cleared orders (every 1 minute)
                            sleep(0.5) # CPU saver!
            else: # no active markets so sleep to save CPU until some new markets appear
                sleep(5)
        # end while
        if not self.session:
            msg = 'SESSION TIMEOUT'
            raise Exception(msg)
