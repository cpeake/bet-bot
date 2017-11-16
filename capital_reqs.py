import os
import logging
import pymongo
from datetime import datetime
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

# Connect to MongoDB
db = MongoClient(MONGODB_URI).get_database()
module_logger.info('Connected to MongoDB: %s' % db)


def calculate_liability(side='', size=0.0, price=0.0):
    if side == 'BACK':
        return size
    else:  # LAY
        return size * (price - 1)


orders = db.orders.find({}).sort([("placedDate", 1)])

pnl = 0.0
capital_req = 0.0

for order in orders:
    sizeSettled = order['sizeSettled']
    priceMatched = order['priceMatched']
    profit = order['profit']
    placedDate = order['placedDate']
    settledDate = order['settledDate']
    side = order['side']
    liability = calculate_liability(side, sizeSettled, priceMatched)
    print("%s, %s, %s" % (placedDate.strftime("%d/%m/%Y %H:%M"), liability, pnl))
    pnl += profit
    print("%s, %s, %s" % (settledDate.strftime("%d/%m/%Y %H:%M"), 0, pnl))