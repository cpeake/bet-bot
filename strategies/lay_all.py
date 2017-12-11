import logging
import betbot_db
import settings
from datetime import datetime
from strategies import helpers

# Set up logging
logger = logging.getLogger('ALS1')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class LayAllStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('ALS1')
        self.state = None
        self.reference = 'ALS1'
        self.init_state()

    def init_state(self):
        self.state = betbot_db.strategy_repo.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategy_repo.upsert({
                'strategyRef': self.reference,
                'name': 'Lay All',
                'weightLadderPosition': 0,
                'daysAtMaxWeight': 0,
                'active': True,
                'updatedDate': datetime.utcnow()
            })

    # Updates the state of the strategy prior to the next (or first) bet being placed.
    # Once a day: If this is the first day of trading or the strategy generated a profit on the previous day,
    #             reset stake weighting to the start of the ladder and days at maximum weight to 0.
    #             If the strategy generated a loss on the previous day, increment the weight ladder position
    #             by 1 if not already at the maximum weight.
    #             If the weight ladder position was already at the maximum weight , increment days at maximum
    #             weight by 1.
    def update_state(self):
        if self.state['updatedDate'] < helpers.get_start_of_day():
            self.logger.info('Updating state at beginning of new day.')
            if helpers.strategy_won_yesterday(self.reference):
                self.logger.info('Won yesterday.')
                if self.state['weightLadderPosition'] > 0:
                    self.state['weightLadderPosition'] -= 1
                    weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                    self.logger.info('Reduced weighting to %sx.' % weight)
                self.state['daysAtMaxWeight'] = 0
                self.logger.info('Reset days at maximum weight to 0.')
            else:
                self.logger.info('Lost yesterday.')
                if self.state['weightLadderPosition'] < (len(settings.weight_ladder) - 1):
                    self.state['weightLadderPosition'] += 1
                    weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                    self.logger.info('Increased weighting to %sx.' % weight)
                else:
                    self.state['daysAtMaxWeight'] += 1
                    self.logger.info('Incremented days at maximum weight to %s.' % self.state['daysAtMaxWeight'])
            betbot_db.strategy_repo.upsert(self.state)

    # Creates a LAY bet on the race favourite at a stake weighted by the daily weighting.
    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            if not self.state['active']:
                self.logger.info('Strategy is not active, no bets generated.')
            else:
                self.update_state()
                stake = helpers.get_stake_by_ladder_position(0)  # fixed staking plan
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                runner = helpers.get_favourite(market_book)
                new_bet = {
                    'customerOrderRef': helpers.get_unique_ref(self.reference),
                    'selectionId': runner['selectionId'],
                    'handicap': 0,
                    'side': 'LAY',
                    'orderType': 'LIMIT',
                    'limitOrder': {
                        'size': stake * weight,
                        'price': helpers.get_lay_limit_price(runner, stake * weight),
                        'persistenceType': 'LAPSE',
                        'timeInForce': 'FILL_OR_KILL'
                    }}
                bets.append(new_bet)
        else:
            msg = 'Failed to create bets for strategy %s, no market/book provided' % self.reference
            raise Exception(msg)
        return bets
