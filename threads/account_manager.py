import logging
import threading
import traceback
from time import sleep

import betbot_db
from strategies import helpers

# Set up logging
logger = logging.getLogger('ACCOM')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Updates Betfair account funds every 10 minutes.
class AccountManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('ACCOM')
        self.api = api

    def run(self):
        self.logger.info('Started Account Manager...')
        while True:
            try:
                self.logger.info("Refreshing account funds.")
                account_funds = self.api.get_account_funds()
                betbot_db.account_funds_repo.upsert(account_funds)
                sleep(10 * 60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Account Manager Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to log in again.
