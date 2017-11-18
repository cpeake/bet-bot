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
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
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
                EmailManager.send_email_with_csv("", "SSS EOD Summary")
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
