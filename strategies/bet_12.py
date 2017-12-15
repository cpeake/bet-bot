import logging
import betbot_db
import settings
from copy import deepcopy
from datetime import datetime
from strategies import helpers

# Set up logging
logger = logging.getLogger('B12S1')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class Bet12Strategy(object):
    def __init__(self):
        self.logger = logging.getLogger('B12S1')
        self.state = {}
        self.reference = 'B12S1'
        self.init_state()
        self.previous_state = deepcopy(self.state)

    def init_state(self):
        self.state = betbot_db.strategy_repo.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategy_repo.upsert({
                    'strategyRef': self.reference,
                    'name': 'Bet 1-2',
                    'stakeLadderPosition': 0,
                    'weightLadderPosition': 0,
                    'betsAtMaxStake': 0,
                    'daysAtMaxWeight': 0,
                    'stopLoss': False,
                    'active': True,
                    'updatedDate': datetime.utcnow()
                })

    # Updates the state of the strategy prior to the next (or first) bet being placed.
    # Once a day:  If this is the first day of trading or the strategy generated a profit on the previous day,
    #              reset weighting to the start of the ladder and days at maximum weight to 0.
    #              If the strategy generated a loss on the previous day, increment the weight ladder position
    #              by 1 if not already at the maximum weight.
    #              If the weight ladder position was already at the maximum weight, increment days at maximum
    #              weight by 1.
    #              Reset the stake ladder and the bets at maximum stake to 0 at the beginning of the day,
    #              regardless of outcome.
    #              Reset the stop loss to False at the beginning of the day, regardless of outcome.
    # Once a race: If the previous race on the day was a LOSS, increment the stake ladder position by 1 if not
    #              already at the maximum weight.
    #              If the previous race on the day was a WIN, reset the stake ladder position and bets at
    #              maximum stake to 0.
    #              If bets at maximum stake reaches 1 (i.e. 1 bet placed at maximum), reset the stake ladder
    #              and bets at maximum stake.
    def update_state(self):
        self.previous_state = deepcopy(self.state)
        if self.state['updatedDate'] < helpers.get_start_of_day():  # Once a day
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
            # Regardless of win or loss yesterday...
            self.state['stakeLadderPosition'] = 0
            self.logger.info('Reset stake ladder.')
            self.state['betsAtMaxStake'] = 0
            self.logger.info('Reset bets at maximum stake to 0.')
            self.state['stopLoss'] = False
            self.logger.info('Removed any stop loss from the previous day.')
        else:  # Once a race
            if helpers.strategy_won_last_market_today(self.reference):
                self.logger.info('Won last race.')
                self.state['stakeLadderPosition'] = 0
                self.logger.info('Reset stake ladder.')
                self.state['betsAtMaxStake'] = 0
                self.logger.info('Reset bets at maximum stake to 0.')
            else:
                self.logger.info('Lost last race.')
                if self.state['stakeLadderPosition'] < (len(settings.stake_ladder) - 1):
                    self.state['stakeLadderPosition'] += 1
                    stake_multiplier = settings.stake_ladder[self.state['stakeLadderPosition']]
                    self.logger.info('Incremented stake ladder to %sx.' % stake_multiplier)
                else:
                    self.state['betsAtMaxStake'] += 1
                    self.logger.info('Incremented bets at maximum stake to %s.' % self.state['betsAtMaxStake'])
                    if self.state['betsAtMaxStake'] == 1:
                        self.state['stakeLadderPosition'] = 0
                        self.state['betsAtMaxStake'] = 0
                        self.logger.info('Previous bet was at maximum stake, reset stake ladder.')
        betbot_db.strategy_repo.upsert(self.state)

    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            if not self.state['active']:
                self.logger.info('Strategy is not active, no bets generated.')
                return bets
            if self.state['stopLoss']:
                self.logger.info('Stop loss triggered, no more bets today.')
                return bets
            self.update_state()
            runner = helpers.get_favourite(market_book)
            if runner:
                stake = helpers.get_stake_by_ladder_position(self.state['stakeLadderPosition'])
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                price = helpers.get_back_limit_price(runner, stake * weight)
                if 2 <= price <= 3:
                    new_bet = {
                        'customerOrderRef': helpers.get_unique_ref(self.reference),
                        'selectionId': runner['selectionId'],
                        'handicap': 0,
                        'side': 'BACK',
                        'orderType': 'LIMIT',
                        'limitOrder': {
                            'size': stake * weight,
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
                self.state = deepcopy(self.previous_state)
                betbot_db.strategy_repo.upsert(self.state)
                self.logger.info("No bet generated, no favourite identified.")
                self.logger.info("Reverted to previous strategy state.")
        else:
            msg = 'Failed to create bets for strategy %s, no market/book provided' % self.reference
            raise Exception(msg)
        return bets
