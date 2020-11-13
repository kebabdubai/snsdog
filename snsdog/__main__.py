import argparse
import csv
import os
import time

from pprint import pprint

import yaml

from snsdog.logs import CloudWatch
from snsdog.sns import SNS


DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.snsdog')
DEFAULT_TIMEOUT_SECS = 25

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A simple Amazon SNS text-message sender with logs')

    parser.add_argument('--phone', required=True, type=str, help='recipient\'s phone number with area code')
    parser.add_argument('--sender', required=False, type=str, help='sender id')
    parser.add_argument('--message', required=True, type=str, help='message text')

    args = parser.parse_args()

    with open(os.path.join(DEFAULT_CONFIG_PATH, 'rootkey.csv'), 'r') as ifs:
        root_key = dict(csv.reader(ifs, delimiter='='))

    with open(os.path.join(DEFAULT_CONFIG_PATH, 'config.yaml'), 'r') as ifs:
        config = yaml.safe_load(ifs)

    region_name = config['aws_region_name']
    log_group_name = config['aws_log_group_name']

    sns = SNS(root_key, region_name)

    response = sns.send(args.phone, args.message, args.sender)
    pprint(response)

    time.sleep(DEFAULT_TIMEOUT_SECS)

    logs = CloudWatch(root_key, region_name)

    for event in logs.get_events_by_message_id(log_group_name,
                                               response['MessageId']):
        pprint(event)
