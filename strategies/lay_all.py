import logging
import betbot_db
import settings
from datetime import datetime
from strategies import helpers


class LayAllStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.strategies.LayAllStrategy')
        self.state = None
        self.reference = 'ALS1'
        self.init_state()

    def init_state(self):
        self.state = betbot_db.strategies.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategies.upsert({
                'strategyRef': self.reference,
                'name': 'All Lay Strategy',
                'weightLadderPosition': 0,
                'daysAtMaxWeight': 0,
                'active': True,
                'updatedDate': datetime.utcnow()
            })

    def strategy_log(self, msg=''):
        msg = ('[%s] ' % self.reference) + msg
        self.logger.info(msg)

    # Updates the state of the strategy prior to the next (or first) bet being placed.
    # Once a day: If this is the first day of trading or the strategy generated a profit on the previous day,
    #             reset stake weighting to the start of the ladder and days at maximum weight to 0.
    #             If the strategy generated a loss on the previous day, increment the weight ladder position
    #             by 1 if not already at the maximum weight.
    #             If the weight ladder position was already at the maximum weight , increment days at maximum
    #             weight by 1.
    def update_state(self):
        if self.state['updatedDate'] < helpers.get_start_of_day():
            self.strategy_log('Updating state at beginning of new day.')
            if helpers.strategy_won_yesterday(self.reference):
                self.strategy_log('Won yesterday.')
                self.state['weightLadderPosition'] = 0
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                self.strategy_log('Reset weighting to %sx.' % weight)
                self.state['daysAtMaxWeight'] = 0
                self.strategy_log('Reset days at maximum weight to 0.')
            else:
                self.strategy_log('Lost yesterday.')
                if self.state['weightLadderPosition'] < (len(settings.stake_ladder) - 1):
                    self.state['weightLadderPosition'] += 1
                    weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                    self.strategy_log('Incremented weighting to %sx.' % weight)
                else:
                    self.state['daysAtMaxWeight'] += 1
                    self.strategy_log('Incremented days at maximum weight to %s.' % self.state['daysAtMaxWeight'])
            betbot_db.strategies.upsert(self.state)

    # Creates a LAY bet on the race favourite at a stake weighted by the daily weighting.
    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            self.update_state()
            stake = helpers.get_stake_by_ladder_position(0)  # fixed staking plan
            weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
            runner = helpers.get_favourite(market_book)
            new_bet = {
                'selectionId': runner['selectionId'],
                'handicap': 0,
                'side': 'LAY',
                'orderType': 'MARKET_ON_CLOSE',
                'marketOnCloseOrder': {
                    'liability': helpers.get_lay_liability(stake * weight, runner['lastPriceTraded'])
                }}
            bets.append(new_bet)
        else:
            msg = 'Failed to create bets for strategy %s, no market provided' % self.reference
            raise Exception(msg)
        return bets
