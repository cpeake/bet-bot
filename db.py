import dateutil.parser
from pymongo import MongoClient
from datetime import datetime, timedelta

# connect to MongoDB
client = MongoClient()
db = client.betbot

orders = db.orders.find({})

for order in orders:
    # convert date strings to datetimes (ISODates in MongoDB)
    placedDate = order['placedDate']
    marketStartTime = order['itemDescription']['marketStartTime']
    settledDate = order['settledDate']
    lastMatchedDate = order['lastMatchedDate']
    if placedDate and type(placedDate) is str:
        order['placedDate'] = dateutil.parser.parse(placedDate)
    if marketStartTime and type(placedDate) is str:
        order['itemDescription']['marketStartTime'] = dateutil.parser.parse(marketStartTime)
    if settledDate and type(settledDate) is str:
        order['settledDate'] = dateutil.parser.parse(settledDate)
    if lastMatchedDate and type(lastMatchedDate) is str:
        order['lastMatchedDate'] = dateutil.parser.parse(lastMatchedDate)
    key = {'betId': order['betId']}
    db.orders.update(key, order, upsert = True)
    
instructions = db.instructions.find({})

for instruction in instructions:
    # convert datetime string to proper datetime for storing as ISODate
    placedDate = instruction['placedDate']
    if placedDate and type(placedDate) is str:
        instruction['placedDate'] = dateutil.parser.parse(placedDate)
    key = {'betId': instruction['betId']}
    db.instructions.update(key, instruction, upsert = True)

markets = db.markets.find({})

for market in markets:
    # convert datetime string to proper datetime for storing as ISODate
    marketStartTime = market['marketStartTime']
    openDate = market['event']['openDate']
    if marketStartTime and type(marketStartTime) is str:
        market['marketStartTime'] = dateutil.parser.parse(marketStartTime)
    if openDate and type(openDate) is str:
        market['event']['openDate'] = dateutil.parser.parse(openDate)
    key = {'marketId': market['marketId']}
    db.markets.update(key, market, upsert = True)
    
market_books = db.market_books.find({})

for market_book in market_books:
    lastMatchTime = market_book['lastMatchTime']
    if lastMatchTime and type(lastMatchTime) is str:
        market_book['lastMatchTime'] = dateutil.parser.parse(lastMatchTime)
    key = {'_id': market_book['_id']}
    db.market_books.update(key, market_book, upsert = True)
    
# Find all cleared orders (status = SETTLED) for this strategy from yesterday and sum the profit.
# if sum(profit) < 0 then False 
now = datetime.utcnow()
today_sod = datetime(now.year, now.month, now.day, 0, 0)
yesterday_sod = today_sod - timedelta(days=1)
orders = db.orders.find({
    "profit": {"$exists": True},
    "customerStrategyRef": "ABS1",
    "settledDate": {'$gte':yesterday_sod,'$lt':today_sod}
})
profit = 0.0
print('Found %s orders for ABS1' % orders.count())
for order in orders:
    profit += order['profit']
print('Profit yesterday: %s' % profit)