import os
import logging
from slackclient import SlackClient

# Set up logging
logger = logging.getLogger('CHATM')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

slack_token = os.environ["SLACK_API_TOKEN"]
channel = os.environ["SLACK_CHANNEL"]
sc = SlackClient(slack_token)
logger.info("Connected to Slack, posting to channel %s." % channel)


class ChatManager(object):
    @staticmethod
    def post_message(msg=''):
        logger.info('Posted message "%s" to Slack channel %s.' % (msg, channel))
        resp = sc.api_call(
          "chat.postMessage",
          channel=channel,
          text=msg
        )
        logger.info(resp)