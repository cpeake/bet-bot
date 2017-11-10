import logging
import threading
import traceback
from time import sleep

# Set up logging
logger = logging.getLogger('SESSM')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Creates the Betfair API session then runs keep alives every 15 minutes as the session
# expires after 20 minutes. Betfair doesn't allow more than one keep alive request in 7 minutes.
class SessionManager(threading.Thread):
    def __init__(self, api, username='', password='', app_key=''):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('SESSM')
        self.api = api
        self.username = username
        self.password = password
        self.api.app_key = app_key
        self.session = False

    def run(self):
        self.logger.info('Started Session Manager...')
        while True:
            try:
                self.do_login()
                sleep(1 * 60)  # Wait for 1 minute before triggering the keep alive loop.
                while self.session:
                    self.keep_alive()
                    sleep(15 * 60)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Session Manager Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to log in again.

    def do_login(self):
        # Logs in to Betfair and sets session status; True on successful login, False otherwise.
        self.session = False
        resp = self.api.login(self.username, self.password)
        if resp == 'SUCCESS':
            self.logger.info('Logged into Betfair API-NG.')
            self.session = True
        else:
            self.logger.error('Failed to log into Betfair API-NG.')
            self.session = False
            msg = 'api.login() resp = %s' % resp
            raise Exception(msg)

    def keep_alive(self):
        # Refreshes the Betfair session and sets status; True on successful keep-alive, False otherwise.
        self.session = False
        resp = self.api.keep_alive()
        if resp == 'SUCCESS':
            self.session = True
        else:
            self.session = False
            msg = 'api.keep_alive() resp = %s' % resp
            raise Exception(msg)
