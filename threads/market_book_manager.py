import logging
import threading
import traceback
from time import sleep
from datetime import datetime
import betbot_db

# Set up logging
logger = logging.getLogger('MABOM')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Captures the market book at 5 second intervals in the 60 seconds before market start time,
# or until the book closes whichever comes first.
# Captures the market book at 30 second intervals after market start time until results are in.
class MarketBookManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.api = api
        self.logger = logging.getLogger('MABOM')

    def run(self):
        self.logger.info('Started Market Book Manager...')
        while True:
            try:
                market = betbot_db.market_repo.get_next()
                market_id = market['marketId']
                venue = market['event']['venue']
                name = market['marketName']
                now = datetime.utcnow()
                start_time = market['marketStartTime']
                delta = (start_time - now).total_seconds()
                if delta < 70:  # Market is going to start within 60 seconds.
                    self.logger.info('Tracking the book for %s %s.' % (venue, name))
                    market_open = True
                    while market_open:
                        market_book = self.api.get_market_book(market_id)
                        betbot_db.market_book_repo.insert(market_book)
                        market_open = market_book['status'] == 'OPEN'
                        sleep(5)
                    market_closed = False
                    while not market_closed:
                        market_book = self.api.get_market_book(market_id)
                        betbot_db.market_book_repo.insert(market_book)
                        market_closed = market_book['status'] == 'CLOSED'
                        sleep(30)
                    self.logger.info('%s %s book has closed, tracking ended.' % (venue, name))
                else:  # wait until a minute before the next market is due to start
                    if delta < 120:
                        self.logger.debug('%s seconds until next market.' % format(delta, '.0f'))
                        sleep(10)
                    else:
                        self.logger.debug('%s minutes until next market.' % format(divmod(delta, 60)[0], '.0f'))
                        sleep(60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Market Book Manager Crashed: %s' % msg)
                # Wait for 1 minute before continuing.
                sleep(1 * 60)


# TODO: How to launch a thread within a thread. Current implementation doesn't support two concurrent markets!
class MarketBookWatcher(threading.Thread):
    def __init__(self, api, market):
        threading.Thread.__init__(self)
        self.api = api
        self.market = market
        self.logger = logging.getLogger('MBW')

    def run(self):
        market_id = self.market['marketId']
        venue = self.market['event']['venue']
        name = self.market['marketName']
        self.logger.info('Tracking the book for %s %s.' % (venue, name))
        try:
            market_open = True
            while market_open:
                market_book = self.api.get_market_book(market_id)
                betbot_db.market_book_repo.insert(market_book)
                market_open = market_book['status'] == 'OPEN'
                sleep(5)
            market_closed = False
            while not market_closed:
                market_book = self.api.get_market_book(market_id)
                betbot_db.market_book_repo.insert(market_book)
                market_closed = market_book['status'] == 'CLOSED'
                sleep(30)
            self.logger.info('%s %s book has closed, tracking ended.' % (venue, name))
        except Exception as exc:
            msg = traceback.format_exc()
            http_err = 'ConnectionError:'
            if http_err in msg:
                msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
            self.logger.error('Market Book Watcher Crashed: %s' % msg)
            # Wait for 1 minute before continuing.
            sleep(1 * 60)
