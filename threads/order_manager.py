import logging
import threading
import traceback
from time import sleep
from datetime import datetime
from strategies import helpers

import betbot_db

# Set up logging
logger = logging.getLogger('ORDEM')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Order Manager receives instructions for market on close and limit orders generated by strategies.
# Market on close order instructions are simply placed and the Betfair API response persisted in the database.
# Limit orders are worked until they are fully executed.
class OrderManager(threading.Thread):
    def __init__(self, api):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('ORDEM')
        self.api = api

    def run(self):
        self.logger.info('Started Order Manager...')
        while True:
            try:
                self.process_live_instructions()
                self.process_simulated_instructions()
                sleep(20)
            except Exception as exc:
                msg = traceback.format_exc()
                http_err = 'ConnectionError:'
                if http_err in msg:
                    msg = '%s%s' % (http_err, msg.rpartition(http_err)[2])
                self.logger.error('Order Manager Crashed: %s' % msg)
                sleep(1 * 60)  # Wait for 1 minute before attempting to log in again.

    def process_live_instructions(self):
        active_instructions = betbot_db.instruction_repo.get_active()
        if active_instructions:
            self.logger.info('Updating order(s) on %s active instruction(s).' % len(active_instructions))
            bet_ids = []
            for bet in active_instructions:
                bet_ids.append(bet['betId'])
            current_orders = self.api.get_current_orders(bet_ids)
            betbot_db.order_repo.upsert(current_orders)
            cleared_orders = self.api.get_cleared_orders(bet_ids)
            betbot_db.order_repo.upsert(cleared_orders)
            betbot_db.instruction_repo.set_settled(cleared_orders)
            # If some orders have cleared, update account balances.
            if len(cleared_orders) > 0:
                account_funds = self.api.get_account_funds()
                betbot_db.account_funds_repo.upsert(account_funds)
            self.delta_update_statistics(cleared_orders)

    def process_simulated_instructions(self):
        instructions = betbot_db.instruction_repo.get_active_simulated()
        if instructions:
            self.logger.info('Updating order(s) on %s SIMULATED active instruction(s).' % len(instructions))
            for instruction in instructions:
                market_id = instruction['marketId']
                selection_id = instruction['instruction']['selectionId']
                strategy_ref = instruction['strategyRef']
                runner_book = self.api.get_runner_book(market_id, selection_id)
                runner_status = runner_book['runners'][0]['status']
                side = instruction['instruction']['side']
                bet_outcome = None
                if side == 'BACK':
                    if runner_status == 'LOSER':
                        bet_outcome = 'LOST'
                    if runner_status == 'WINNER':
                        bet_outcome = 'WON'
                else:  # LAY
                    if runner_status == 'LOSER':
                        bet_outcome = 'WON'
                    if runner_status == 'WINNER':
                        bet_outcome = 'LOST'
                order = {
                    'marketId': market_id,
                    'selectionId': selection_id,
                    'eventTypeId': 7,
                    'betId': instruction['betId'],
                    'orderType': instruction['instruction']['orderType'],
                    'side': side,
                    'placedDate': instruction['placedDate'],
                    'itemDescription': {
                        'eventDesc': market_id,
                        'runnerDesc': selection_id,
                        'marketStartTime': instruction['marketStartTime']
                    },
                    'customerStrategyRef': strategy_ref,
                    'simulated': True
                }
                if bet_outcome:
                    size = instruction['instruction']['limitOrder']['size']
                    price = instruction['instruction']['limitOrder']['price']
                    order['betOutcome'] = bet_outcome
                    order['settledDate'] = datetime.utcnow()
                    order['sizeSettled'] = size
                    order['priceMatched'] = price
                    order['profit'] = self.calculate_profit(side, size, price, bet_outcome)
                betbot_db.order_repo.upsert([order])
                if 'settledDate' in order:
                    betbot_db.instruction_repo.set_settled([order])
                    self.delta_update_statistics([order])

    # TODO: Check calculation for LAY profit/loss
    # TODO: Factor in Betfair commission including point reduction
    def calculate_profit(self, side='', size=0.0, price=0.0, bet_outcome=''):
        if side == 'BACK':
            if bet_outcome == 'WON':
                return size * (price - 1)
            else:  # LOST
                return size * -1.0
        else:  # side == 'LAY'
            factor = 1.0 if bet_outcome == 'WON' else -1.0
            return size * (price - 1.0) * factor

    def delta_update_statistics(self, cleared_orders):
        self.logger.info('Doing a delta strategy statistics update.')
        strategy_pnls = {}
        for cleared_order in cleared_orders:
            strategy_ref = cleared_order['customerStrategyRef']
            if strategy_ref in strategy_pnls:
                strategy_pnls[strategy_ref] += cleared_order['profit']
            else:
                strategy_pnls[strategy_ref] = cleared_order['profit']
        for strategy_ref, pnl in strategy_pnls.items():
            statistics = betbot_db.statistic_repo.get_by_reference(strategy_ref)
            if statistics['updatedDate'] < helpers.get_start_of_day():
                statistics['dailyPnL'] = 0.0
            statistics['dailyPnL'] += pnl
            statistics['weeklyPnL'] += pnl
            statistics['monthlyPnL'] += pnl
            statistics['yearlyPnL'] += pnl
            statistics['lifetimePnL'] += pnl
            betbot_db.statistic_repo.upsert(statistics)
