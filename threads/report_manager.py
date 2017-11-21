import logging
import threading
import traceback
from time import sleep, time
from datetime import timedelta

from comms import EmailManager
from strategies import helpers
import betbot_db

# Set up logging
logger = logging.getLogger('REPOM')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class ReportManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('REPOM')
        self.api = api

    def run(self):
        self.logger.info('Started Report Manager...')
        while True:
            try:
                self.logger.info("Sending T-1 summary email.")
                csv = "PlacedDate,PlacedTime,SettledDate,SettledTime,Strategy,Market,Runner,Side,Stake,Price,Outcome,PnL\n"
                strategies = betbot_db.strategy_repo.get_all()
                for strategy in strategies:
                    strategy_ref = strategy['strategyRef']
                    orders = betbot_db.order_repo.get_settled_yesterday_by_strategy(strategy_ref)
                    for order in orders:
                        market = betbot_db.market_repo.get_by_id(order['marketId'])
                        runner = betbot_db.runner_repo.get_by_id(order['selectionId'])
                        venue = market['event']['venue']
                        race = market['marketName']
                        market_name = "%s %s" % (venue, race)
                        runner_name = runner['runnerName']
                        placed = order['placedDate']
                        settled = order['settledDate']
                        placed_date = placed.strftime('%d-%b-%Y')
                        placed_time = placed.strftime('%H:%M:%S')
                        settled_date = settled.strftime('%d-%b-%Y')
                        settled_time = settled.strftime('%H:%M:%S')
                        side = order['side']
                        size = order['sizeSettled']
                        price = order['priceMatched']
                        outcome = order['betOutcome']
                        profit = order['profit']
                        csv += "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (placed_date, placed_time, settled_date, settled_time, strategy_ref, market_name, runner_name, side, size, price, outcome, profit)
                EmailManager.send_email_with_csv("", "SSS EOD Summary", csv)
                now = time()
                tomorrow1am = helpers.get_tomorrow_start_of_day() + timedelta(hours=1)
                sleep(tomorrow1am.timestamp() - now)  # Wait until 01:00 tomorrow.
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Report Manager Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to restart
