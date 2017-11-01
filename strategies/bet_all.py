import logging


class BetAllStrategy(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.strategies.BetAllStrategy')

    def create_bets(self, market=None, market_book=None):
        bets = []
        # if this is the first day of trading or the strategy generated a profit on the previous day,
        # reset the stake weighting ladder once at the beginning of the day.
        # if the strategy generated a loss on the previous day, increment the stake weighting ladder once
        # at the beginning of the day
        # reset the stake ladder at the beginning of the day regardless of outcome.
        # if the previous race on the day was a LOSS, increment the stake ladder for the next bet
        # if the previous race on the day was a WIN, reset the stake ladder for the next bet
        strategy_bets = []
        if market:
            # work out current state of strategy
            strategy_state = self.get_strategy_state('ABS1')
            if not strategy_state:  # if there isn't a strategy state, intialise one
                strategy_state = {
                    'strategy_ref': 'ABS1',
                    'stake_ladder_position': 0,
                    'weight_ladder_position': 0,
                    'max_stake_count': 0,
                    'updatedDate': datetime.utcnow()
                }
                self.upsert_strategy_state(strategy_state)
            else:
                # recalculate new strategy state if not already done so today
                now = datetime.utcnow()
                today_sod = datetime(now.year, now.month, now.day, 0, 0)
                if strategy_state['updatedDate'] < today_sod and not self.strategy_won_yesterday('ABS1'):
                    # increment the weight ladder position if possible
                    if strategy_state['weight_ladder_position'] < (len(settings.stake_ladder) - 1):
                        strategy_state['weight_ladder_position'] += 1
                        self.upsert_strategy_state(strategy_state)
                        # recalculate new strategy state if there is a previous result today
                        # if self.strategy_lost_last_market_today('ABS1'):
                        # increment the stake ladder position if possible
                        #    if strategy_state['stake_ladder_position'] < (len(settings.stake_ladder) - 1):
                        #        strategy_state['stake_ladder_position'] += 1
                        #    else:  # already at stake limit, track for stop loss
                        #        strategy_state['max_stake_count'] += 1

            book = self.get_market_book(market)
            self.insert_market_book(book)
            runner = self.get_favourite(book)
            stake = 2.0
            new_bet = {
                'selectionId': runner['selectionId'],
                'handicap': 0,
                'side': 'BACK',
                'orderType': 'MARKET_ON_CLOSE',
                'marketOnCloseOrder': {
                    'liability': stake
                }}
            strategy_bets.append(new_bet)
        return bets
