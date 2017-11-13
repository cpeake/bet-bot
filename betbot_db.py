import os
import logging
import dateutil.parser
import pymongo
from datetime import datetime, timedelta
from pymongo import MongoClient
from strategies import helpers

module_logger = logging.getLogger('betbot_application.betbot_db')

MONGODB_URI = os.environ['MONGODB_URI']

if not MONGODB_URI:
    module_logger.error('MONGODB_URI is not set, exiting.')
    exit()

# Connect to MongoDB
db = MongoClient(MONGODB_URI).get_database()
module_logger.info('Connected to MongoDB: %s' % db)


class MarketRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.MarketRepository')

    def get_by_id(self, market_id=''):
        self.logger.debug('Retrieving market %s' % market_id)
        return db.markets.find_one({
            "marketId": market_id
        })

    def upsert(self, market=None):
        if market:
            # convert datetime strings to proper date times for storing as ISODate
            market_start_time = market['marketStartTime']
            open_date = market['event']['openDate']
            if market_start_time and type(market_start_time) is str:
                market['marketStartTime'] = dateutil.parser.parse(market_start_time)
            if open_date and type(open_date) is str:
                market['event']['openDate'] = dateutil.parser.parse(open_date)
            # Pull out the runners and upsert separately.
            runners = market.pop('runners', None)
            key = {'marketId': market['marketId']}
            db.markets.update(key, market, upsert=True)
            if runners and type(runners) is list:
                for runner in runners:
                    runner_repo.upsert(runner)
        else:
            msg = 'Failed to upsert a market, None provided.'
            raise Exception(msg)

    def set_played(self, market=None):
        """set the provided market as played (i.e. bets have been placed successfully)"""
        if market:
            market['played'] = True
            self.upsert(market)
        else:
            msg = 'Failed to set a market as played, None provided.'
            raise Exception(msg)

    def set_skipped(self, market=None, error_code=''):
        """set the provided market as skipped (i.e. bets have not been placed successfully)
           either because the market was in the past when betting was attempted, or
           an attempt to place bets failed
        """
        if market:
            market['played'] = False
            if error_code:
                market['errorCode'] = error_code
            self.upsert(market)
        else:
            msg = 'Failed to set a market as played, None provided.'
            raise Exception(msg)

    def get_next_playable(self):
        """returns the next playable market"""
        return db.markets.find({
            "played": {"$exists": False}
        }).sort([('marketStartTime', 1)]).next()

    def get_next(self):
        # Gets the next market(s) by time. If there are two markets starting at exactly the
        # same time, both are returned.
        future_markets = db.markets.find({
            'marketStartTime': {'$gt': datetime.utcnow()}
        }).sort([('marketStartTime', 1)])
        if future_markets.count() > 0:
            next_market = future_markets.next()
            market_start_time = next_market['marketStartTime']
            next_markets = []
            for market in db.markets.find({'marketStartTime': market_start_time}):
                next_markets.append(market)
            return next_markets
        else:
            return None

    def get_most_recently_played(self):
        return db.markets.find({
            "played": {"$exists": True}
        }).sort([('marketStartTime', -1)]).next()


class MarketBookRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.MarketBookRepository')

    def insert(self, market_book=None):
        if market_book:
            # convert datetime strings to proper date times for storing as ISODate
            if 'lastMatchTime' in market_book:
                last_match_time = market_book['lastMatchTime']
                if last_match_time and type(last_match_time) is str:
                    market_book['lastMatchTime'] = dateutil.parser.parse(last_match_time)
            # add a snapshot datetime
            market_book['snapshotTime'] = datetime.utcnow()
            db.market_books.insert_one(market_book)
        else:
            msg = 'Failed to insert a market book, None provided.'
            raise Exception(msg)


class RunnerBookRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.RunnerBookRepository')

    def upsert(self, runner_book=None):
        if runner_book:
            # do datetime conversions first
            key = {
                'marketId': runner_book['marketId'],
                'selectionId': runner_book['selectionId']
            }
            db.markets.update(key, runner_book, upsert=True)
        else:
            msg = 'Failed to upsert a runner book, None provided.'
            raise Exception(msg)


class RunnerRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.RunnerRepository')

    def upsert(self, runner=None):
        if runner:
            # Remove unnecessary keys if they exist.
            runner.pop('handicap', None)
            runner.pop('sortPriority', None)
            key = {'selectionId': runner['selectionId']}
            db.runners.update(key, runner, upsert=True)

    def get_by_id(self, selection_id=''):
        return db.runners.find_one({'selectionId': selection_id})


class InstructionRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.InstructionRepository')

    def get_by_id(self, bet_id=''):
        self.logger.debug('Retrieving instruction %s' % bet_id)
        return db.instructions.find_one({'betId': bet_id})

    def insert(self, market=None, instruction=None):
        if market and instruction:
            market_id = market['marketId']
            # convert datetime string to proper datetime for storing as ISODate
            placed_date = instruction['placedDate']
            if placed_date and type(placed_date) is str:
                instruction['placedDate'] = dateutil.parser.parse(placed_date)
            instruction['marketId'] = market_id
            instruction['settled'] = False
            db.instructions.insert_one(instruction)
        else:
            msg = 'Failed to insert an instruction, None provided.'
            raise Exception(msg)

    def upsert(self, instruction=None):
        if instruction:
            # convert datetime string to proper datetime for storing as ISODate
            placed_date = instruction['placedDate']
            if placed_date and type(placed_date) is str:
                instruction['placedDate'] = dateutil.parser.parse(placed_date)
            key = {'betId': instruction['betId']}
            db.instructions.update(key, instruction, upsert=True)

    def get_active(self):
        """returns live instructions not marked as settled"""
        bets = []
        for bet in db.instructions.find({"settled": False, "live": {"$in": [True, None]}}):
            bets.append(bet)
        return bets

    def get_active_simulated(self):
        """returns simulated instructions not marked as settled"""
        bets = []
        for bet in db.instructions.find({"settled": False, "live": False}):
            bets.append(bet)
        return bets

    def set_settled(self, cleared_orders=None):
        if cleared_orders is None:
            cleared_orders = []
        for order in cleared_orders:
            bet_id = order['betId']
            self.logger.info('Marking instruction %s as settled.' % bet_id)
            instruction = self.get_by_id(bet_id)
            if instruction:
                instruction['settled'] = True
                self.upsert(instruction)
            else:
                msg = 'Instruction %s not found.' % bet_id
                raise Exception(msg)


class OrderRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.OrderRepository')

    def upsert(self, order_list=None):
        if order_list is None:
            order_list = []
        self.logger.debug('Upserting %s orders' % len(order_list))
        for order in order_list:
            self.logger.debug(order)
            # convert date strings to datetimes (ISODates in MongoDB)
            placed_date = order['placedDate']
            market_start_time = None
            if 'itemDescription' in order:
                market_start_time = order['itemDescription']['marketStartTime']
            settled_date = None
            if 'settledDate' in order:
                settled_date = order['settledDate']
            matched_date = None
            if 'matchedDate' in order:
                matched_date = order['matchedDate']
            if placed_date and type(placed_date) is str:
                order['placedDate'] = dateutil.parser.parse(placed_date)
            if market_start_time and type(market_start_time) is str:
                order['itemDescription']['marketStartTime'] = dateutil.parser.parse(market_start_time)
            if settled_date and type(settled_date) is str:
                order['settledDate'] = dateutil.parser.parse(settled_date)
            if matched_date and type(matched_date) is str:
                order['matchedDate'] = dateutil.parser.parse(matched_date)
            key = {'betId': order['betId']}
            db.orders.update(key, order, upsert=True)

    def get_settled_yesterday_by_strategy(self, strategy_ref=''):
        now = datetime.utcnow()
        today_sod = datetime(now.year, now.month, now.day, 0, 0)
        yesterday_sod = today_sod - timedelta(days=1)
        return list(db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": strategy_ref,
            "settledDate": {'$gte': yesterday_sod, '$lt': today_sod}
        }))

    def get_latest_settled_by_strategy(self, strategy_ref=''):
        settled_orders = db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": strategy_ref
        }).sort('settledDate', pymongo.DESCENDING).limit(1)
        if settled_orders.count() > 0:
            return settled_orders[0]
        else:
            return None

    def get_latest_settled_today_by_strategy(self, strategy_ref):
        now = datetime.utcnow()
        today_sod = datetime(now.year, now.month, now.day, 0, 0)
        today_eod = datetime(now.year, now.month, now.day, 23, 59)
        settled_orders = db.orders.find({
            "profit": {"$exists": True},
            "customerStrategyRef": strategy_ref,
            "settledDate": {'$gte': today_sod, '$lt': today_eod}
        }).sort('settledDate', pymongo.DESCENDING).limit(1)
        if settled_orders.count() > 0:
            return settled_orders[0]
        else:
            return None

    def get_pnls(self, start_date=None):
        pipeline = []
        if type(start_date) is datetime:
            pipeline.append({'$match': {'placedDate': {'$gt': start_date}}})
        pipeline.append({'$match': {'profit': {'$exists': True}}})
        pipeline.append({'$group': {'_id': '$customerStrategyRef', 'pnl': {'$sum': '$profit'}}})
        pnls = db.orders.aggregate(pipeline)
        strategy_pnls = {}
        for pnl in pnls:
            strategy_pnls[pnl['_id']] = pnl['pnl']
        return strategy_pnls

    def get_daily_pnls(self):
        return self.get_pnls(helpers.get_start_of_day())

    def get_wtd_pnls(self):
        return self.get_pnls(helpers.get_start_of_week())

    def get_mtd_pnls(self):
        return self.get_pnls(helpers.get_start_of_month())

    def get_ytd_pnls(self):
        return self.get_pnls(helpers.get_start_of_year())

    def get_lifetime_pnls(self):
        return self.get_pnls()


class StrategyRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.StrategyRepository')

    def get_by_reference(self, strategy_ref=''):
        self.logger.debug('Getting strategy with reference %s' % strategy_ref)
        return db.strategies.find_one({'strategyRef': strategy_ref})

    def is_live(self, strategy_ref=''):
        strategy = db.strategies.find_one({'strategyRef': strategy_ref})
        return 'live' in strategy and strategy['live']

    def upsert(self, strategy_state=None):
        if strategy_state:
            strategy_state['updatedDate'] = datetime.utcnow()
            key = {'strategyRef': strategy_state['strategyRef']}
            db.strategies.update(key, strategy_state, upsert=True)


class StatisticRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.StatisticsRepository')

    def get_all(self):
        return db.statistics.find({})

    def get_by_reference(self, strategy_ref=''):
        statistic = db.statistics.find_one({'strategyRef': strategy_ref})
        if not statistic:
            statistic = {
                'strategyRef': strategy_ref,
                'dailyPnL': 0.0,
                'weeklyPnL': 0.0,
                'monthlyPnL': 0.0,
                'ytdPnL': 0.0,
                'lifetimePnL': 0.0,
                'updatedDate': datetime.utcnow()
            }
            self.upsert(statistic)
        return statistic

    def upsert(self, statistic=None):
        if statistic:
            statistic['updatedDate'] = datetime.utcnow()
            key = {'strategyRef': statistic['strategyRef']}
            db.statistics.update(key, statistic, upsert=True)


class AccountFundsRepository(object):
    def __init__(self):
        self.logger = logging.getLogger('betbot_application.betbot_db.AccountRepository')

    def upsert(self, account_funds=None):
        if account_funds:
            account_funds['updatedDate'] = datetime.utcnow()
            key = {'wallet': account_funds['wallet']}
            db.account_funds.update(key, account_funds, upsert=True)


market_repo = MarketRepository()
runner_repo = RunnerRepository()
market_book_repo = MarketBookRepository()
runner_book_repo = RunnerBookRepository()
instruction_repo = InstructionRepository()
order_repo = OrderRepository()
strategy_repo = StrategyRepository()
statistic_repo = StatisticRepository()
account_funds_repo = AccountFundsRepository()