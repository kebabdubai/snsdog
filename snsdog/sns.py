from typing import Any, Dict, Optional

import boto3


class SNS:

    def __init__(self, root_key: Dict[str, str], region_name: str) -> None:
        self._session = boto3.client('sns',
                                     aws_access_key_id=root_key['AWSAccessKeyId'],
                                     aws_secret_access_key=root_key['AWSSecretKey'],
                                     region_name=region_name)

    def send(self, phone_number: str, message: str, sender_id: Optional[str] = None) -> Dict[str, Any]:  # check None
        response = self._session.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SenderID': {
                    'DataType': 'String',
                    'StringValue': sender_id
                }
            }
        )

        return response
