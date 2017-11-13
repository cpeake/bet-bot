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

    def watch_market_book(self, market=None):
        market_id = market['marketId']
        venue = market['event']['venue']
        name = market['marketName']
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

    def run(self):
        self.logger.info('Started Market Book Manager...')
        while True:
            try:
                next_markets = betbot_db.market_repo.get_next()
                if len(next_markets) > 0:
                    now = datetime.utcnow()
                    start_time = next_markets[0]['marketStartTime']
                    delta = (start_time - now).total_seconds()
                    if delta < 70:  # Market is going to start within 60 seconds.
                        for market in next_markets:
                            thread_name = 'MBW-%s' % market['marketId']
                            mbw = threading.Thread(target=self.watch_market_book, name=thread_name, args=market)
                            mbw.start()
                        sleep(2 * 60)  # Until after the market start time has passed.
                    else:  # Start counting down towards the next market.
                        if delta < 120:
                            self.logger.debug('%s seconds until next market(s).' % format(delta, '.0f'))
                            sleep(10)
                        else:
                            self.logger.debug('%s minutes until next market(s).' % format(divmod(delta, 60)[0], '.0f'))
                            sleep(60)
                else:
                    self.logger.info('No next market(s) available.')
                    sleep(5 * 60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Market Book Manager Crashed: %s' % msg)
                # Wait for 1 minute before continuing.
                sleep(1 * 60)
