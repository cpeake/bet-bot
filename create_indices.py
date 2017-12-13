import os
import logging
import pymongo
from pymongo import MongoClient

# Set up logging
logger = logging.getLogger('betbot_application')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

module_logger = logging.getLogger('betbot_application.betbot_db')

MONGODB_URI = os.environ['MONGODB_URI']

if not MONGODB_URI:
    module_logger.error('MONGODB_URI is not set, exiting.')
    exit()


def index_exists(name='', indices=None):
    return name in indices


# Connect to MongoDB
db = MongoClient(MONGODB_URI).get_database()
module_logger.info('Connected to MongoDB: %s' % db)

# Create indices on collection 'markets'

market_indices = db.markets.index_information()

if index_exists('marketId', market_indices):
    db.markets.drop_index('marketId')
db.markets.create_index([('marketId', pymongo.DESCENDING)], name='marketId', unique=True)

if not index_exists('marketStartTime', market_indices):
    db.markets.create_index([('marketStartTime', pymongo.DESCENDING)], name='marketStartTime')

if not index_exists('played', market_indices):
    db.markets.create_index([('played', pymongo.ASCENDING)], name='played')

if not index_exists('errorCode', market_indices):
    db.markets.create_index([('errorCode', pymongo.ASCENDING)], name='errorCode')

# Create indices on collection 'market_books'

market_book_indices = db.market_books.index_information()

if not index_exists('marketId', market_book_indices):
    db.market_books.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

# Create indices on collection 'runner_books'

runner_book_indices = db.runner_books.index_information()

if not index_exists('marketId', runner_book_indices):
    db.runner_books.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

# Create indices on collection 'runner_book_results'

runner_book_result_indices = db.runner_book_results.index_information()

if not index_exists('marketId', runner_book_result_indices):
    db.runner_book_results.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

# Create indices on collection 'instructions'

instruction_indices = db.instructions.index_information()

if not index_exists('marketId', instruction_indices):
    db.instructions.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

if index_exists('betId', instruction_indices):
    db.instructions.drop_index('betId')
db.instructions.create_index([('betId', pymongo.DESCENDING)], name='betId', unique=True)

# Create indices on collection 'orders'

order_indices = db.orders.index_information()

if not index_exists('marketId', order_indices):
    db.orders.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

if not index_exists('selectionId', order_indices):
    db.orders.create_index([('selectionId', pymongo.ASCENDING)], name='selectionId')

if not index_exists('placedDate', order_indices):
    db.orders.create_index([('placedDate', pymongo.DESCENDING)], name='placedDate')

if not index_exists('settledDate', order_indices):
    db.orders.create_index([('settledDate', pymongo.DESCENDING)], name='settledDate')

if not index_exists('customerStrategyRef', order_indices):
    db.orders.create_index([('customerStrategyRef', pymongo.ASCENDING)], name='customerStrategyRef')

if not index_exists('profit', order_indices):
    db.orders.create_index([('profit', pymongo.ASCENDING)], name='profit')

# Create indices on collection 'runners'

runner_indices = db.runners.index_information()

if index_exists('selectionId', runner_indices):
    db.runners.drop_index('selectionId')
db.runners.create_index([('selectionId', pymongo.DESCENDING)], name='selectionId', unique=True)

# Create indices on collection 'winners'

winner_indices = db.winners.index_information()

if index_exists('marketId', winner_indices):
    db.winners.drop_index('marketId')
db.winners.create_index([('marketId', pymongo.DESCENDING)], name='marketId')

if index_exists('selectionId', winner_indices):
    db.winners.drop_index('selectionId')
db.winners.create_index([('selectionId', pymongo.DESCENDING)], name='selectionId')
