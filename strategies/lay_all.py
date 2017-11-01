import datetime
import logging
import betbot_db


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
                'strategy_ref': 'ALS1',
                'name': 'All Lay Strategy',
                'weight_ladder_position': 0,
                'updatedDate': datetime.utcnow()
            })

    def update_state(self):
        if self.state['updatedDate']

    def create_bets(self, market=None, market_book=None):
        # if this is the first day of trading or the strategy generated a profit on the previous day,
        # place a lay bet at minimum stake (converted to liability) in the stake ladder, else place a lay bet with
        # the next stake (converted to liability) in the stake ladder.
        bets = []
        if market:
            # work out current state of strategy
            strategy_state = self.get_strategy_state('ALS1')
            if not strategy_state:  # if there isn't a strategy state, intialise one
                strategy_state = {'strategy_ref': 'ALS1', 'stake_ladder_position': 0, 'updatedDate': datetime.utcnow()}
                self.upsert_strategy_state(strategy_state)
            else:
                # recalculate new strategy state if not already done so today
                now = datetime.utcnow()
                today_sod = datetime(now.year, now.month, now.day, 0, 0)
                if strategy_state['updatedDate'] < today_sod and not self.strategy_won_yesterday('ALS1'):
                    # increment the stake ladder position if possible
                    if strategy_state['stake_ladder_position'] < (len(settings.stake_ladder) - 1):
                        strategy_state['stake_ladder_position'] += 1
                        self.upsert_strategy_state(strategy_state)
            stake = self.get_stake_by_ladder_position(strategy_state['stake_ladder_position'])
            book = self.get_market_book(market)
            betbot_db.market_books.insert(book)
            runner = self.get_favourite(book)
            new_bet = {
                'selectionId': runner['selectionId'],
                'handicap': 0,
                'side': 'LAY',
                'orderType': 'MARKET_ON_CLOSE',
                'marketOnCloseOrder': {
                    'liability': self.get_lay_liability(stake, runner['lastPriceTraded'])
                }}
            strategy_bets.append(new_bet)
        self.update_state()

        return bets
