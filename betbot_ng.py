__version__ = 0.01

import os
import logging
import betbot_db
import strategies
from time import time, sleep
from betfair.api_ng import API
from datetime import datetime
from slackclient import SlackClient

module_logger = logging.getLogger('betbot_application.betbot_ng')


class BetBot(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_ng.BetBot')
        self.username = ''  # set by run() function at startup
        self.api = None  # set by run() function at startup
        self.sc = None  # set by run() function at startup
        self.sim_mode = False  # set by run() function at startup
        self.slack_channel = os.environ['SLACK_CHANNEL']
        self.throttle = {
            'next': time(),  # time we can send next request. auto-updated in do_throttle()
            'wait': 1.0,  # time in seconds between requests
            'keep_alive': time(),  # auto-updated in keep_alive()
            'update_orders': time(),  # auto-updated in update_orders()
            'refresh_markets': time()  # auto-updated in refresh_markets()
        }
        self.bet_all_strategy = strategies.BetAllStrategy()
        self.lay_all_strategy = strategies.LayAllStrategy()
        self.session = False

    def do_throttle(self):
        """return when it's safe to continue"""
        now = time()
        if now < self.throttle['next']:
            wait = self.throttle['next'] - now
            sleep(wait)
        self.throttle['next'] = time() + self.throttle['wait']
        return

    def do_login(self, username='', password=''):
        """login to Betfair & set session status"""
        self.session = False
        resp = self.api.login(username, password)
        if resp == 'SUCCESS':
            self.session = True
        else:
            self.session = False  # failed login
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
                self.throttle['keep_alive'] = now + (15 * 60)  # add 15 mins
                self.session = True
            else:
                self.session = False
                msg = 'api.keep_alive() resp = %s' % resp
                raise Exception(msg)

    def refresh_markets(self):
        """refresh available markets every 15 minutes"""
        now = time()
        if now > self.throttle['refresh_markets']:
            self.logger.info('Refreshing list of markets.')
            # define the filter
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
            # send the request
            markets = self.api.get_markets(params)
            if type(markets) is list:  # upsert into the DB
                self.logger.info('Retrieved %s markets.' % len(markets))
                for market in markets:
                    betbot_db.markets.upsert(market)
            else:
                msg = 'Failed to retrieve markets: resp = %s' % markets
                raise Exception(msg)
            # update throttle to refresh again in 15 minutes
            self.throttle['refresh_markets'] = now + (15 * 60)  # add 15 mins

    def update_orders(self):
        now = time()
        if now > self.throttle['update_orders']:
            active_instructions = betbot_db.instructions.get_active()
            if active_instructions:
                self.logger.info('Updating order(s) on %s active instruction(s).' % len(active_instructions))
                bet_ids = []
                for bet in active_instructions:
                    bet_ids.append(bet['betId'])
                current_orders = self.api.get_current_orders(bet_ids)
                betbot_db.orders.upsert(current_orders)
                cleared_orders = self.api.get_cleared_orders(bet_ids)
                betbot_db.orders.upsert(cleared_orders)
                betbot_db.instructions.set_settled(cleared_orders)
            # update throttle to refresh again in 1 minutes
            self.throttle['update_orders'] = now + 60  # add 1 min

    def get_market_book(self, market=None):
        books = self.api.get_market_books([market['marketId']])
        if type(books) is list:
            betbot_db.market_books.insert(books[0])
            return books[0]
        else:
            msg = 'Failed to get market book: resp = %s' % books
            raise Exception(msg)

    def create_bets(self, market=None):
        market_book = self.get_market_book(market)
        return {
            self.bet_all_strategy.reference: self.bet_all_strategy.create_bets(market, market_book),
            self.lay_all_strategy.reference: self.lay_all_strategy.create_bets(market, market_book)
        }

    def place_bets(self, market=None, market_bets=None):
        """place bets for all strategies on a given market"""
        venue = market['event']['venue']
        name = market['marketName']
        if market_bets:
            for strategy_ref in market_bets:
                market_bets = market_bets[strategy_ref]
                resp = self.api.place_bets(market['marketId'], market_bets, strategy_ref)
                if type(resp) is dict and 'status' in resp:
                    if resp['status'] == 'SUCCESS':
                        # set the market as played
                        betbot_db.markets.set_played(market)
                        # persist the instructions
                        for instruction in resp['instructionReports']:
                            betbot_db.instructions.insert(market, instruction)
                        self.logger.info('Successfully placed bet(s) on %s %s.' % (venue, name))
                    else:
                        self.logger.error(
                            'Failed to place bet(s) on %s %s. (Error: %s)' % (venue, name, resp['errorCode']))
                        # set the market as skipped, it's too late to try again
                        betbot_db.markets.set_skipped(market, resp['errorCode'])
                else:
                    msg = 'Failed to place bet(s) on %s %s - resp = %s' % (venue, name, resp)
                    raise Exception(msg)

    def post_slack_message(self, msg=''):
        if msg:
            self.sc.api_call(
                "chat.postMessage",
                channel=self.slack_channel,
                text=msg
            )

    def run(self, username='', password='', app_key=''):
        # Create the API object and login
        self.username = username
        self.api = API(False, ssl_prefix=username)  # connect to the UK (rather than AUS) API
        self.api.app_key = app_key
        self.do_login(username, password)
        # login to Slack
        self.sc = SlackClient(os.environ["SLACK_API_TOKEN"])
        self.post_slack_message('BetBot has started! :tada:')
        while self.session:
            self.do_throttle()  # base level of loop frequency (every 1 second)
            self.keep_alive()  # refresh Betfair API-NG login session (every 15 minutes)
            self.refresh_markets()  # refresh available markets (every 15 minutes)
            self.update_orders()  # update current and cleared orders (every 1 minute)
            next_market = betbot_db.markets.get_next_playable()
            if next_market:
                name = next_market['marketName']
                venue = next_market['event']['venue']
                start_time = next_market['marketStartTime']
                self.logger.info('Next market is the %s %s at %s.' % (venue, name, start_time))
                now = datetime.utcnow()
                wait = (start_time - now).total_seconds()
                if wait < 0:  # "next" market is in the past and can't be played
                    msg = "%s %s has already started, skipping." % (venue, name)
                    self.logger.warning(msg)
                    betbot_db.markets.set_skipped(next_market, 'MARKET_IN_PAST')
                else:
                    if wait < 60:  # process the next market, less than a minute to go before start
                        strategy_bets = self.create_bets(next_market)
                        if strategy_bets:
                            self.logger.info('Generated bets on %s %s.\n%s' % (venue, name, strategy_bets))
                            # self.place_bets(next_market, strategy_bets)
                            betbot_db.markets.set_skipped(next_market, 'SIMULATION_MODE')  # comment when live
                        else:
                            self.logger.info('No bets generated on %s %s, skipping.')
                            betbot_db.markets.set_skipped(next_market, 'NO_BETS_CREATED')
                    else:  # wait until a minute before the next market is due to start
                        self.logger.info("Sleeping until 1 minute before %s %s starts." % (venue, name))
                        time_target = time() + wait - 60
                        while time() < time_target:
                            self.keep_alive()  # refresh login session (runs every 15 mins)
                            self.update_orders()  # update current and cleared orders (every 1 minute)
                            sleep(0.5)  # CPU saver!
            else:  # no active markets so sleep to save CPU until some new markets appear
                sleep(5)
        # end while
        if not self.session:
            msg = 'SESSION TIMEOUT'
            raise Exception(msg)
