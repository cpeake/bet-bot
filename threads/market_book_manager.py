import logging
import threading
import traceback
from time import sleep
from datetime import datetime
import betbot_db
from strategies import helpers

# Set up logging
logger = logging.getLogger('MABOM')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Captures the market book at 1 second intervals from 5 seconds before market start until just before the
# book closes. The intention is to capture an early indication of the outcome based on final in-play odds.
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
        market_closed = False
        while not market_closed:
            # Whichever runner has lay prices < 2, select as the indicative winner.
            market_book = self.api.get_market_book(market_id)
            market_closed = market_book['status'] == 'CLOSED'
            if not market_closed:
                runner = helpers.get_indicative_winner(market_book)
                if runner:
                    selection_id = runner['selectionId']
                    winner = {'marketId': market_id, 'selectionId': selection_id}
                    self.logger.debug("Constructed winner: %s" % winner)
                    betbot_db.winners_repo.upsert(winner)
            sleep(1)
        self.logger.info('%s %s book has closed, tracking ended.' % (venue, name))

    def run(self):
        self.logger.info('Started Market Book Manager...')
        while True:
            try:
                next_markets = betbot_db.market_repo.get_next()
                if next_markets and len(next_markets) > 0:
                    now = datetime.utcnow()
                    start_time = next_markets[0]['marketStartTime']
                    delta = (start_time - now).total_seconds()
                    if delta < 5:  # Market is going to start within 5 seconds.
                        for market in next_markets:
                            thread_name = 'MBW-%s' % market['marketId']
                            mbw = threading.Thread(target=self.watch_market_book, name=thread_name, args=(market, ))
                            mbw.start()
                        sleep(2 * 60)  # Until after the market start time has passed.
                    else:
                        self.logger.info("Sleeping until 5 seconds before next market(s) start(s).")
                        sleep(delta - 5)
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
