import settings
import betbot_db


def get_stake_by_ladder_position(position=0):
    return settings.stake_ladder[position] * settings.minimum_stake * settings.stake_multiplier


def get_lay_liability(stake=0.0, price=0.0):
    """get the lay liability based on the provided stake and price"""
    return stake * (price - 1.0)


def get_market_book(self, market=None):
    books = self.api.get_market_books([market['marketId']])
    if type(books) is list:
        return books[0]
    else:
        msg = 'Failed to get market book: resp = %s' % books
        raise Exception(msg)


def strategy_won_yesterday(strategy_ref=''):
    # Find all cleared orders (profit attribute exists) for this strategy from yesterday and sum the profit.
    # if sum(profit) < 0 then False otherwise True (profit >= 0 or no cleared orders)
    orders = betbot_db.orders.get_settled_yesterday_by_strategy(strategy_ref)
    if orders.count() == 0:
        return True  # if there are no orders default to True
    profit = 0.0
    for order in orders:
        profit += order['profit']
    return profit >= 0.0


def strategy_won_last_market(strategy_ref=''):
    """return True if the most recent bet made by the strategy WON
       or if there is no previous bet made by the strategy, False otherwise"""
    order = betbot_db.orders.get_latest_settled_by_strategy(strategy_ref)
    if order:
        if order['betOutcome'] == 'WON':
            return True
        else:
            return False
    else:
        return True


def strategy_won_last_market_today(strategy_ref=''):
    """return True if the most recent bet made by the strategy WON
       or if there is no previous bet made by the strategy, False otherwise"""
    order = betbot_db.orders.get_latest_settled_today_by_strategy(strategy_ref)
    if order:
        if order['betOutcome'] == 'WON':
            return True
        else:
            return False
    else:
        return True


def get_favourite(market_book=None):
    favourite = None
    best_price = float("inf")
    for runner in market_book['runners']:
        if runner['status'] == 'ACTIVE':  # the horse has got to be running still!
            if 'lastPriceTraded' in runner:  # sometimes there isn't a lastPriceTraded available
                if runner['lastPriceTraded'] < best_price:
                    favourite = runner
                    best_price = runner['lastPriceTraded']
    return favourite
