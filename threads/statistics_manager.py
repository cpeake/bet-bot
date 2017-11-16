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
                daily_total = 0.0
                wtd_total = 0.0
                mtd_total = 0.0
                ytd_total = 0.0
                lifetime_total = 0.0
                for statistic in statistics:
                    strategy_ref = statistic['strategyRef']
                    daily_pnl = daily_pnls[strategy_ref] if strategy_ref in daily_pnls else 0
                    wtd_pnl = wtd_pnls[strategy_ref] if strategy_ref in wtd_pnls else 0
                    mtd_pnl = mtd_pnls[strategy_ref] if strategy_ref in mtd_pnls else 0
                    ytd_pnl = ytd_pnls[strategy_ref] if strategy_ref in ytd_pnls else 0
                    lifetime_pnl = lifetime_pnls[strategy_ref] if strategy_ref in lifetime_pnls else 0
                    daily_total += daily_pnl
                    wtd_total += wtd_pnl
                    mtd_total += mtd_pnl
                    ytd_total += ytd_pnl
                    lifetime_total += lifetime_pnl
                    statistic['dailyPnL'] = daily_pnl
                    statistic['weeklyPnL'] = wtd_pnl
                    statistic['monthlyPnL'] = mtd_pnl
                    statistic['yearlyPnL'] = ytd_pnl
                    statistic['lifetimePnL'] = lifetime_pnl
                    betbot_db.statistic_repo.upsert(statistic)
                total_statistic = betbot_db.statistic_repo.get_by_reference('TOTALS')
                total_statistic['dailyPnL'] = daily_total
                total_statistic['weeklyPnL'] = wtd_total
                total_statistic['monthlyPnL'] = mtd_total
                total_statistic['yearlyPnL'] = ytd_total
                total_statistic['lifetimePnL'] = lifetime_total
                betbot_db.statistic_repo.upsert(total_statistic)

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
