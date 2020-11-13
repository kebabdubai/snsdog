import heapq
import operator

from typing import Dict, Any, Optional

import boto3


class CloudWatch:

    def __init__(self, root_key: Dict[str, str], region_name: str):
        self._session = boto3.client('logs',
                                     aws_access_key_id=root_key['AWSAccessKeyId'],
                                     aws_secret_access_key=root_key['AWSSecretKey'],
                                     region_name=region_name)

    def get_last_events(self, log_group_name: str, log_stream_limit: int,
                        log_event_limit: int) -> Any:  # refactor, return type, follow next pages

        response = self._session.describe_log_streams(
            logGroupName=log_group_name,
            limit=log_stream_limit,
            orderBy='LastEventTime',
            descending=True)

        last_events = []
        first_event_timestamp = -1

        for log_stream in response['logStreams']:
            if log_stream['lastEventTimestamp'] <= first_event_timestamp:
                if len(last_events) < log_event_limit:
                    first_event_timestamp = min(first_event_timestamp, log_stream['firstEventTimestamp'])
                else:
                    break

            response = self._session.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream['logStreamName'])

            last_events.extend((-entry['timestamp'], entry) for entry in response['events'])

        heapq.heapify(last_events)

        yield from map(operator.itemgetter(1),
                       (heapq.heappop(last_events) for _ in range(min(len(last_events), log_event_limit))))

    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
    def get_events_by_message_id(self, log_group_name: str, message_id: str, log_event_limit: Optional[int] = None) -> Any:  # return type
        response = self._session.filter_log_events(
            logGroupName=log_group_name,
            limit=log_event_limit,
            filterPattern=f'{{ $.notification.messageId = "{message_id}" }}')

        yield from response['events']
