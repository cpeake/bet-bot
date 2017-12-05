import logging
import threading
import traceback
from time import sleep
from datetime import datetime

import betbot_db
import strategies
from comms import ChatManager
from strategies import helpers

# Set up logging
logger = logging.getLogger('STRAM')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class StrategyManager(threading.Thread):
    def __init__(self, api, live_mode=False):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('STRAM')
        self.api = api
        self.live_mode = live_mode
        self.bet_all_strategy = strategies.BetAllStrategy()
        self.lay_all_strategy = strategies.LayAllStrategy()
        self.bet_12_strategy = strategies.Bet12Strategy()
        self.bet_odds_strategy = strategies.BetOddsStrategy()

    def run(self):
        self.logger.info('Started Strategy Manager...')
        self.logger.info('Strategy Manager is in %s mode!' % ('LIVE' if self.live_mode else 'SIMULATION'))
        while True:
            try:
                next_market = betbot_db.market_repo.get_next_playable()
                if next_market:
                    name = next_market['marketName']
                    venue = next_market['event']['venue']
                    start_time = next_market['marketStartTime']
                    self.logger.info('Next market is the %s %s at %s.' % (venue, name, start_time))
                    now = datetime.utcnow()
                    wait = (start_time - now).total_seconds()
                    if wait < 0:  # "Next" market is in the past and can't be played
                        msg = "%s %s has already started, skipping." % (venue, name)
                        self.logger.warning(msg)
                        betbot_db.market_repo.set_skipped(next_market, 'MARKET_IN_PAST')
                    else:
                        if wait < 30:  # Process the next market, less than a minute to go before start!
                            strategy_bets = self.create_bets(next_market)
                            if strategy_bets:
                                self.logger.info('Generated bets on %s %s.' % (venue, name))
                                self.logger.info(strategy_bets)
                                self.place_bets(next_market, strategy_bets)
                            else:
                                self.logger.info('No bets generated on %s %s, skipping.')
                                betbot_db.market_repo.set_skipped(next_market, 'NO_BETS_CREATED')
                        else:  # wait until a minute before the next market is due to start
                            self.logger.info("Sleeping until 30 seconds before %s %s starts." % (venue, name))
                            sleep(wait - 30)
                else:  # No active markets so sleep to save CPU until some new markets appear.
                    sleep(1 * 60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Strategy Manager Crashed: %s' % msg)
                sleep(1 * 60)

    def get_market_book(self, market_id=''):
        market_book = betbot_db.market_book_repo.get_recent_snapshot(market_id)
        if not market_book:
            market_book = self.api.get_market_book(market_id)
            betbot_db.market_book_repo.insert(market_book)
        return market_book

    def create_bets(self, market=None):
        market_book = self.get_market_book(market['marketId'])
        return {
            self.bet_all_strategy.reference: self.bet_all_strategy.create_bets(market, market_book),
            self.lay_all_strategy.reference: self.lay_all_strategy.create_bets(market, market_book),
            self.bet_12_strategy.reference: self.bet_12_strategy.create_bets(market, market_book),
            self.bet_odds_strategy.reference: self.bet_odds_strategy.create_bets(market, market_book)
        }

    def place_bets(self, market=None, market_bets=None):
        """place bets for all strategies on a given market"""
        venue = market['event']['venue']
        name = market['marketName']
        if market_bets:
            for strategy_ref, strategy_bets in market_bets.items():
                live_strategy = betbot_db.strategy_repo.is_live(strategy_ref)
                if len(strategy_bets) > 0:
                    if self.live_mode and live_strategy:
                        resp = self.api.place_bets(market['marketId'], strategy_bets, strategy_ref)
                        self.logger.info(resp)
                        # self.post_bets(market, strategy_bets, strategy_ref)
                    else:
                        resp = self.simulate_place_bets(market, strategy_bets, strategy_ref)
                        # self.post_bets(market, strategy_bets, strategy_ref)
                    if type(resp) is dict and 'status' in resp:
                        if resp['status'] == 'SUCCESS':
                            # Set the market as played.
                            betbot_db.market_repo.set_played(market)
                            # Persist the instructions.
                            for instruction in resp['instructionReports']:
                                betbot_db.instruction_repo.insert(market, instruction)
                            self.logger.info('Successfully placed %s bet(s) on %s %s.' % (strategy_ref, venue, name))
                        else:
                            self.logger.error(
                                'Failed to place %s bet(s) on %s %s. (Error: %s)' %
                                (strategy_ref, venue, name, resp['errorCode']))
                            # Set the market as skipped, it's too late to try again.
                            betbot_db.market_repo.set_skipped(market, resp['errorCode'])
                    else:
                        msg = 'Failed to place %s bet(s) on %s %s - resp = %s' % (strategy_ref, venue, name, resp)
                        raise Exception(msg)

    def simulate_place_bets(self, market=None, strategy_bets=None, strategy_ref=''):
        self.logger.debug('Simulating receipt of instruction reports.')
        resp = {'status': 'SUCCESS', 'instructionReports': []}
        if strategy_bets:
            for strategy_bet in strategy_bets:
                instruction_report = {
                    'status': 'SUCCESS',
                    'instruction': strategy_bet,
                    'placedDate': datetime.utcnow(),
                    'betId': '%s-%s-%s' % (strategy_ref, market['marketId'], strategy_bet['selectionId']),
                    'averagePriceMatched': strategy_bet['limitOrder']['price'],
                    'sizeMatched': strategy_bet['limitOrder']['size'],
                    'marketId': market['marketId'],
                    'marketStartTime': market['marketStartTime'],
                    'strategyRef': strategy_ref,
                    'live': False
                }
                resp['instructionReports'].append(instruction_report)
        return resp

    def post_bets(self, market, strategy_bets=[], strategy_ref=''):
        strategy = betbot_db.strategy_repo.get_by_reference(strategy_ref)
        strategy_name = strategy['name']
        venue = market['event']['venue']
        market_name = market['marketName']
        for strategy_bet in strategy_bets:
            selection_id = strategy_bet['selectionId']
            runner = betbot_db.runner_repo.get_by_id(selection_id)
            runner_name = runner['runnerName']
            price = strategy_bet['limitOrder']['price'],
            size = strategy_bet['limitOrder']['size'],
            msg = '%s placed an order on %s in the %s %s for £%s @ %s.' %\
                  (strategy_name, runner_name, venue, market_name, size, price)
            ChatManager.post_message(msg)
