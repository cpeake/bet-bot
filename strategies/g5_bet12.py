import logging
import betbot_db
from copy import deepcopy
from datetime import datetime
from strategies import helpers

# Set up logging
logger = logging.getLogger('G5B12')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class Group5Bet12Strategy(object):
    def __init__(self):
        self.logger = logging.getLogger('G5B12')
        self.state = {}
        self.reference = 'G5B12'
        self.init_state()
        self.previous_state = deepcopy(self.state)

    def init_state(self):
        self.state = betbot_db.strategy_repo.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategy_repo.upsert({
                    'strategyRef': self.reference,
                    'name': 'Group 5 Bet 1-2',
                    'groupPosition': -1,
                    'startingStake': 100,
                    'stakeLadder': [1, 1, 2, 2, 4],
                    'stop': False,
                    'active': True,
                    'updatedDate': datetime.utcnow()
                })

    def update_state(self):
        self.previous_state = deepcopy(self.state)
        self.state['groupPosition'] += 1
        if self.state['groupPosition'] > 0 and helpers.strategy_won_last_market(self.reference):
            self.state['stop'] = True
        self.logger.info('Incremented group position to %s.' % self.state['groupPosition'])
        if self.state['groupPosition'] == 5:
            self.logger.info('Reached end of race grouping. Reset group position and stop.')
            self.state['groupPosition'] = 0
            self.state['stop'] = False
        betbot_db.strategy_repo.upsert(self.state)

    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            if not self.state['active']:
                self.logger.info('Strategy is not active, no bets generated.')
                return bets
            self.update_state()
            if self.state['stop']:
                self.logger.info('Group stop triggered, no more bets in this race grouping.')
                return bets
            runner = helpers.get_favourite(market_book)
            if runner:
                stake = self.state['startingStake'] * self.state['stakeLadder'][self.state['groupPosition']]
                price = helpers.get_back_limit_price(runner, stake)
                if not self.state['stop']:
                    if 2.0 <= price <= 3.0:
                        new_bet = {
                            'customerOrderRef': helpers.get_unique_ref(self.reference),
                            'selectionId': runner['selectionId'],
                            'handicap': 0,
                            'side': 'BACK',
                            'orderType': 'LIMIT',
                            'limitOrder': {
                                'size': stake,
                                'price': price,
                                'persistenceType': 'LAPSE',
                                'timeInForce': 'FILL_OR_KILL'
                            }}
                        bets.append(new_bet)
                    else:
                        self.state = deepcopy(self.previous_state)
                        betbot_db.strategy_repo.upsert(self.state)
                        self.logger.info("No bet generated, favourite price is not between 1-2 (2-3 on Betfair).")
                        self.logger.info("Reverted to previous strategy state.")
                else:
                    self.logger.info("No bet generated, group stop in place due to group win.")
            else:
                self.state = deepcopy(self.previous_state)
                betbot_db.strategy_repo.upsert(self.state)
                self.logger.info("No bet generated, no favourite identified.")
                self.logger.info("Reverted to previous strategy state.")
        else:
            msg = 'Failed to create bets for strategy %s, no market/book provided' % self.reference
            raise Exception(msg)
        return bets
