import os
import logging
import settings
import time
import betbot_db
from datetime import datetime, timedelta

module_logger = logging.getLogger('betbot_application.betbot_db')


class MarketDepthError(Exception):
    def __init__(self, depth=0.0, stake=0.0, msg=None):
        if msg is None:
            msg = "Insufficient market depth £%s found for stake £%s." % (depth, stake)
        super(MarketDepthError, self).__init__(msg)
        self.depth = depth
        self.stake = stake


def get_log_level():
    level = logging.DEBUG
    if 'LOG_LEVEL' in os.environ:
        log_env = os.environ['LOG_LEVEL']
        if log_env == 'DEBUG':
            level = logging.DEBUG
        elif log_env == 'INFO':
            level = logging.INFO
        elif log_env == 'WARN':
            level = logging.WARN
        else:
            level = logging.ERROR
    return level


def get_stake_by_ladder_position(position=0):
    return settings.stake_ladder[position] * settings.minimum_stake * settings.stake_multiplier


def get_weight_by_ladder_position(position=0):
    return settings.weight_ladder[position]


def get_lay_liability(stake=0.0, price=0.0):
    """get the lay liability based on the provided stake and price"""
    module_logger.info('Calculating lay liability: {stake: %s, price: %s}' % (stake, price))
    return stake * (price - 1.0)


def strategy_won_yesterday(strategy_ref=''):
    # Find all cleared orders (profit attribute exists) for this strategy from yesterday and sum the profit.
    # if sum(profit) < 0 then False otherwise True (profit >= 0 or no cleared orders)
    orders = betbot_db.order_repo.get_settled_yesterday_by_strategy(strategy_ref)
    if len(orders) == 0:
        return True  # if there are no orders default to True
    profit = 0.0
    for order in orders:
        profit += order['profit']
    return profit >= 0.0


def strategy_won_last_market(strategy_ref=''):
    """return True if the most recent bet made by the strategy WON
       or if there is no previous bet made by the strategy, False otherwise"""
    module_logger.info("Getting last settled order for strategy %s" % strategy_ref)
    order = betbot_db.order_repo.get_latest_settled_by_strategy(strategy_ref)
    module_logger.info(order)
    if order:
        if order['betOutcome'] == 'LOST':
            return False
        else:
            return True
    else:
        return True


def strategy_won_last_market_today(strategy_ref=''):
    """return True if the most recent bet made by the strategy WON
       or if there is no previous bet made by the strategy, False otherwise"""
    order = betbot_db.order_repo.get_latest_settled_today_by_strategy(strategy_ref)
    if order:
        if 'betOutcome' in order and order['betOutcome'] == 'LOST':
            return False
        else:
            return True
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


def get_indicative_winner(market_book=None):
    for runner in market_book['runners']:
        market_levels = runner['ex']['availableToLay']
        if len(market_levels) > 0:
            price = market_levels[0]['price']
            if price < 1.5:
                return runner
    return None


def get_back_limit_price(runner=None, stake=0.0):
    if runner:
        market_levels = runner['ex']['availableToBack']
        return get_limit_price(market_levels, stake)
    else:
        return 0.0


def get_lay_limit_price(runner=None, stake=0.0):
    if runner:
        market_levels = runner['ex']['availableToLay']
        return get_limit_price(market_levels, stake)
    else:
        return 0.0


def get_limit_price(market_levels=None, stake=0.0):
    if market_levels:
        depth = 0.0
        for market_level in market_levels:
            depth += market_level['size']
            if depth > stake:
                return market_level['price']
        msg = "Insufficient market depth found for a stake of £%s." % stake
        raise MarketDepthError(depth, stake, msg)
    else:
        return 0.0


def get_start_of_day():
    now = datetime.utcnow()
    return datetime(now.year, now.month, now.day, 0, 0)


def get_tomorrow_start_of_day():
    now = datetime.utcnow()
    sod = datetime(now.year, now.month, now.day, 0, 0)
    return sod + timedelta(days=1)


def get_start_of_week():
    now = datetime.utcnow()
    sod = datetime(now.year, now.month, now.day, 0, 0)
    return sod - timedelta(days=(datetime.today().isoweekday() % 7) - 1)


def get_start_of_month():
    now = datetime.utcnow()
    return datetime(now.year, now.month, 1, 0, 0)


def get_start_of_year():
    now = datetime.utcnow()
    return datetime(now.year, 1, 1, 0, 0)


def get_unique_ref(ref):
    return "%s.%s" % (ref, int(round(time.time() * 1000)))
