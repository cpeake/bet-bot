import urllib3
import threading
import logging
import traceback
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep

import betbot_db
from strategies import helpers

# Set up logging
logger = logging.getLogger('RESUS')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Disable non-HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ResultScraper(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('RESUS')

    def run(self):
        self.logger.info('Started Result Scraper...')
        while True:
            try:
                self.logger.info("Scraping results from sportinglife.com")
                http = urllib3.PoolManager()
                url = 'https://www.sportinglife.com/racing/fast-results'
                response = http.request('GET', url)
                soup = BeautifulSoup(response.data, "html.parser")

                race_cards = soup.find_all('div', attrs={'class': 'fast-racecard-item'})

                self.logger.debug("Found %s results on sportinglife.com" % len(race_cards))
                for race_card in race_cards:
                    card_header = race_card.find('div', attrs={'class': 'fast-racecard-header-race'})
                    race = card_header.text.split()
                    race_time = race[0].split(":")
                    now = datetime.utcnow()
                    start_time = datetime(now.year, now.month, now.day, int(race_time[0]), int(race_time[1]))
                    venue = race[1]
                    market = betbot_db.market_repo.get_by_time_and_venue(start_time, venue)
                    if market:
                        places = race_card.find_all('div', attrs={'class': 'fast-results-place'})
                        self.logger.debug("Found results for %s %s" % (market['event']['venue'], market['marketName']))
                        if len(places) > 0:
                            runner = places[0].find('div', attrs={'class': 'fast-results-place-name'}).text
                            runner_name = runner.split('(')[0].rstrip().replace("'", "")
                            try:
                                runner = betbot_db.runner_repo.get_by_name(runner_name)
                                winner = {'marketId': market['marketId'], 'selectionId': runner['selectionId']}
                                self.logger.debug("Constructed winner: %s" % winner)
                                betbot_db.winners_repo.upsert(winner)
                            except Exception:
                                self.logger.warning("Failed to find runner matching %s" % runner_name)
                    else:
                        self.logger.debug("Failed to find market matching %s %s." % (race_time, venue))
                sleep(30)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Result Scraper Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to log in again.


