import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from strategies import helpers

# Set up logging
logger = logging.getLogger('EMAILM')
logger.setLevel(helpers.get_log_level())
ch = logging.StreamHandler()
ch.setLevel(helpers.get_log_level())
formatter = logging.Formatter('(%(name)s) - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

smtp_host = os.environ['SMTP_HOSTNAME']
smtp_port = os.environ['SMTP_PORT']
smtp_user = os.environ['SMTP_USERNAME']
smtp_pass = os.environ['SMTP_PASSWORD']
recipients = os.environ['EMAIL_RECIPIENTS']


class EmailManager(object):
    def __init__(self):
        self.logger = logger
        self.s = smtplib.SMTP(host=smtp_host, port=smtp_port)

    def smtp_connect(self):
        self.s.starttls()
        self.s.login(smtp_user, smtp_pass)
        self.logger.info("Connected to SMTP server %s@%s:%s" % (smtp_user, smtp_host, smtp_port))

    def smtp_disconnect(self):
        self.s.quit()
        self.logger.info("Disconnected to SMTP server %s@%s:%s" % (smtp_user, smtp_host, smtp_port))

    def send_email_with_csv(self, message='', subject='', csv=''):
        msg = MIMEMultipart()  # create a message

        # setup the parameters of the message
        msg['From'] = smtp_user
        msg['To'] = recipients
        msg['Subject'] = subject

        # add in the message body
        msg.attach(MIMEText(message, 'plain'))

        attachment = MIMEText(csv, 'csv')
        attachment.add_header("Content-Disposition", "attachment", filename="summary.csv")

        msg.attach(attachment)

        # send the message via the server set up earlier.
        self.smtp_connect()
        self.s.send_message(msg)
        self.smtp_disconnect()
