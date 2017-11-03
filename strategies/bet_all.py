import logging
import betbot_db
import settings
from datetime import datetime
from strategies import helpers


class BetAllStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.strategies.BetAllStrategy')
        self.state = None
        self.reference = 'ABS1'
        self.init_state()

    def init_state(self):
        self.state = betbot_db.strategies.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategies.upsert({
                    'strategyRef': self.reference,
                    'stakeLadderPosition': 0,
                    'weightLadderPosition': 0,
                    'betsAtMaxStake': 0,
                    'daysAtMaxWeight': 0,
                    'stopLoss': False,
                    'active': True,
                    'updatedDate': datetime.utcnow()
                })

    def strategy_log(self, msg=''):
        msg = ('[%s] ' % self.reference) + msg
        self.logger.info(msg)

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
    #              If bets at maximum stake reaches 4 (i.e. 3 bets placed at maximum), set stop loss to True.
    def update_state(self):
        if self.state['updatedDate'] < helpers.get_start_of_day():  # Once a day
            self.strategy_log('Updating state at beginning of new day.')
            if helpers.strategy_won_yesterday(self.reference):
                self.strategy_log('Won yesterday.')
                self.state['weightLadderPosition'] = 0
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                self.strategy_log('Reset weighting to %sx.' % weight)
                self.state['daysAtMaxWeight'] = 0
                self.strategy_log('Reset days at maximum weight to 0.')
            else:
                self.logger.info('Lost yesterday.')
                if self.state['weightLadderPosition'] < (len(settings.weight_ladder) - 1):
                    self.state['weightLadderPosition'] += 1
                    weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                    self.strategy_log('Incremented weighting to %sx.' % weight)
                else:
                    self.state['daysAtMaxWeight'] += 1
                    self.strategy_log('Incremented days at maximum weight to %s.' % self.state['daysAtMaxWeight'])
            # Regardless of win or loss yesterday...
            self.state['stakeLadderPosition'] = 0
            self.strategy_log('Reset stake ladder.')
            self.state['betsAtMaxStake'] = 0
            self.strategy_log('Reset bets at maximum stake to 0.')
            self.state['stopLoss'] = False
            self.strategy_log('Removed any stop loss from the previous day.')
        else:  # Once a race
            if helpers.strategy_won_last_market_today(self.reference):
                self.strategy_log('Won last race.')
                self.state['stakeLadderPosition'] = 0
                self.strategy_log('Reset stake ladder.')
                self.state['betsAtMaxStake'] = 0
                self.strategy_log('Reset bets at maximum stake to 0.')
            else:
                self.strategy_log('Lost last race.')
                if self.state['stakeLadderPosition'] < (len(settings.stake_ladder) - 1):
                    self.state['stakeLadderPosition'] += 1
                    stake_multiplier = settings.stake_ladder[self.state['stakeLadderPosition']]
                    self.strategy_log('Incremented stake ladder to %sx.' % stake_multiplier)
                else:
                    self.state['betsAtMaxStake'] += 1
                    self.strategy_log('Incremented bets at maximum stake to %s.' % self.state['betsAtMaxStake'])
                    if self.state['betsAtMaxStake'] == 1:
                        self.state['stopLoss'] = True
                        self.strategy_log('Stop loss triggered, %s race(s) lost at maximum stake.' % self.state['betsAtMaxStake'])
        betbot_db.strategies.upsert(self.state)

    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            self.update_state()
            if self.state['stopLoss']:
                self.strategy_log('Stop loss triggered, no more bets today.')
            else:
                runner = helpers.get_favourite(market_book)
                stake = helpers.get_stake_by_ladder_position(self.state['stakeLadderPosition'])
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                new_bet = {
                    'selectionId': runner['selectionId'],
                    'handicap': 0,
                    'side': 'BACK',
                    'orderType': 'MARKET_ON_CLOSE',
                    'marketOnCloseOrder': {
                        'liability': stake * weight
                    }}
                bets.append(new_bet)
        return bets
