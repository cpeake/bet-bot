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
s = smtplib.SMTP(host=smtp_host, port=smtp_port)
s.starttls()
s.login(smtp_user, smtp_pass)
logger.info("Connected to SMTP server %s@%s:%s" % (smtp_user, smtp_host, smtp_port))


class EmailManager(object):
    @staticmethod
    def send_email_with_csv(message='', subject='', csv=''):
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
        s.send_message(msg)
