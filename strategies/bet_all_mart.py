import logging
import betbot_db
import settings
from datetime import datetime
from strategies import helpers


class BetAllMartStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.strategies.BetAllMartStrategy')
        self.state = None
        self.reference = 'BMS1'
        self.init_state()

    def init_state(self):
        self.state = betbot_db.strategy_repo.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategy_repo.upsert({
                    'strategyRef': self.reference,
                    'weightLadderPosition': 0,
                    'daysAtMaxWeight': 0,
                    'betLossStreak': 0,
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
                    self.strategy_log('Incrementing weight ladder.')
                    self.state['weightLadderPosition'] += 1
                else:
                    self.strategy_log('Not incrementing weight ladder, already at maximum.')
                    self.strategy_log('Incrementing days at maximum weight.')
                    self.state['daysAtMaxWeight'] += 1
            # Regardless of win or loss yesterday...
            self.state['betLossStreak'] = 0
            self.strategy_log('Reset bet loss streak to 0.')
            self.state['stopLoss'] = False
            self.strategy_log('Removed any stop loss from the previous day.')
        else:  # Once a race
            if helpers.strategy_won_last_market_today(self.reference):
                self.strategy_log('Won last race.')
                self.strategy_log('Resetting stake ladder.')
                self.state['stakeLadderPosition'] = 0
                self.strategy_log('Resetting bets at maximum stake to 0.')
                self.state['betsAtMaxStake'] = 0
            else:
                self.strategy_log('Lost last race.')
                if self.state['stakeLadderPosition'] < (len(settings.stake_ladder) - 1):
                    self.strategy_log('Incrementing stake ladder.')
                    self.state['stakeLadderPosition'] += 1
                else:
                    self.strategy_log('Incrementing bets at maximum stake.')
                    self.state['betsAtMaxStake'] += 1
                    if self.state['betsAtMaxStake'] == 4:
                        self.strategy_log('Triggering stop loss, 3 races lost at maximum stake.')
                        self.state['stopLoss'] = True
        betbot_db.strategy_repo.upsert(self.state)

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
