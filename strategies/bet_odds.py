import logging
import betbot_db
import settings
from datetime import datetime
from strategies import helpers

# Set up logging
logger = logging.getLogger('BOS1')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class BetOddsStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('BOS1')
        self.state = None
        self.reference = 'BOS1'
        self.init_state()

    def init_state(self):
        self.state = betbot_db.strategy_repo.get_by_reference(self.reference)
        if not self.state:  # no state available, create an initial state
            self.state = betbot_db.strategy_repo.upsert({
                    'strategyRef': self.reference,
                    'name': 'Bet Odds',
                    'lostStakeSum': 0,
                    'weightLadderPosition': 0,
                    'sequentialLosses': 0,
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
        if self.state['updatedDate'] < helpers.get_start_of_day():  # Once a day
            self.logger.info('Updating state at beginning of new day.')
            if helpers.strategy_won_yesterday(self.reference):
                self.logger.info('Won yesterday.')
                self.state['weightLadderPosition'] = 0
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                self.logger.info('Reset weighting to %sx.' % weight)
                self.state['daysAtMaxWeight'] = 0
                self.logger.info('Reset days at maximum weight to 0.')
            else:
                self.logger.info('Lost yesterday.')
                if self.state['weightLadderPosition'] < (len(settings.weight_ladder) - 1):
                    self.state['weightLadderPosition'] += 1
                    weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                    self.logger.info('Incremented weighting to %sx.' % weight)
                else:
                    self.state['daysAtMaxWeight'] += 1
                    self.logger.info('Incremented days at maximum weight to %s.' % self.state['daysAtMaxWeight'])
            # Regardless of win or loss yesterday...
            self.state['sequentialLosses'] = 0
            self.logger.info('Reset sequential losses to 0.')
            self.state['lostStakeSum'] = 0
            self.logger.info('Reset lost stake sum to 0.')
            self.state['stopLoss'] = False
            self.logger.info('Removed any stop loss from the previous day.')
        else:  # Once a race
            if helpers.strategy_won_last_market_today(self.reference):
                self.logger.info('Won last race.')
                self.state['sequentialLosses'] = 0
                self.logger.info('Reset sequential losses to 0.')
                self.state['lostStakeSum'] = 0
                self.logger.info('Reset lost stake sum to 0.')
            else:
                self.logger.info('Lost last race.')
                self.state['sequentialLosses'] += 1
                self.logger.info('Incremented sequential losses by 1.')
                if self.state['sequentialLosses'] == 3:
                    self.state['stopLoss'] = True
                    self.logger.info('5 races lost in a row, triggering stop loss.')
                last_order = betbot_db.order_repo.get_latest_today_by_strategy(self.reference)
                if last_order:
                    last_instruction = betbot_db.instruction_repo.get_by_id(last_order['betId'])
                    if last_instruction:
                        last_stake = last_instruction['instruction']['limitOrder']['size']
                        self.state['lostStakeSum'] += last_stake
                        self.logger.info('Incremented lost stake sum by £%s' % last_stake)
                    else:
                        self.logger.info('Failed to increment last stake sum, no previous instruction found.')
                else:
                    self.logger.info('Last stake sum not incremented, no previous order found.')
        betbot_db.strategy_repo.upsert(self.state)

    def create_bets(self, market=None, market_book=None):
        bets = []
        if market and market_book:
            if not self.state['active']:
                self.logger.info('Strategy is not active, no bets generated.')
                return bets
            self.update_state()
            if self.state['stopLoss']:
                self.logger.info('Stop loss triggered, no more bets today.')
            else:
                runner = helpers.get_favourite(market_book)
                adjusted_last_price = runner['lastPriceTraded'] - 1
                if self.state['lostStakeSum'] == 0:
                    stake = settings.minimum_stake
                else:
                    stake = (self.state['lostStakeSum'] + adjusted_last_price) / adjusted_last_price
                weight = helpers.get_weight_by_ladder_position(self.state['weightLadderPosition'])
                new_bet = {
                    'customerOrderRef': helpers.get_unique_ref(self.reference),
                    'selectionId': runner['selectionId'],
                    'handicap': 0,
                    'side': 'BACK',
                    'orderType': 'LIMIT',
                    'limitOrder': {
                        'size': stake * weight,
                        'price': helpers.get_back_limit_price(runner, stake * weight),
                        'persistenceType': 'LAPSE',
                        'timeInForce': 'FILL_OR_KILL'
                    }}
                bets.append(new_bet)
        else:
            msg = 'Failed to create bets for strategy %s, no market/book provided' % self.reference
            raise Exception(msg)
        return bets
