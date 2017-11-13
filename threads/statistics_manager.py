import logging
import threading
import traceback
from time import time, sleep
from datetime import timedelta

import betbot_db
from strategies import helpers

# Set up logging
logger = logging.getLogger('STATM')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Creates the Betfair API session then runs keep alives every 15 minutes as the session
# expires after 20 minutes. Betfair doesn't allow more than one keep alive request in 7 minutes.
class StatisticsManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('STATM')
        self.api = api

    def run(self):
        self.logger.info('Started Statistics Manager...')
        while True:
            try:
                self.logger.info('Doing a full statistics update.')
                statistics = betbot_db.statistic_repo.get_all()
                daily_pnls = betbot_db.order_repo.get_daily_pnls()
                wtd_pnls = betbot_db.order_repo.get_wtd_pnls()
                mtd_pnls = betbot_db.order_repo.get_mtd_pnls()
                ytd_pnls = betbot_db.order_repo.get_ytd_pnls()
                lifetime_pnls = betbot_db.order_repo.get_lifetime_pnls()
                for statistic in statistics:
                    strategy_ref = statistic['strategyRef']
                    statistic['dailyPnL'] = daily_pnls[strategy_ref] if strategy_ref in daily_pnls else 0
                    statistic['weeklyPnL'] = wtd_pnls[strategy_ref] if strategy_ref in wtd_pnls else 0
                    statistic['monthlyPnL'] = mtd_pnls[strategy_ref] if strategy_ref in mtd_pnls else 0
                    statistic['yearlyPnL'] = ytd_pnls[strategy_ref] if strategy_ref in ytd_pnls else 0
                    statistic['lifetimePnL'] = lifetime_pnls[strategy_ref] if strategy_ref in lifetime_pnls else 0
                    betbot_db.statistic_repo.upsert(statistic)

                now = time()
                tomorrow1am = helpers.get_tomorrow_start_of_day() + timedelta(hours=1)
                sleep(tomorrow1am.timestamp() - now)  # Wait until 01:00 tomorrow.
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Statistics Manager Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to log in again.
