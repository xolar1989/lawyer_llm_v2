import os

from botocore.exceptions import NoCredentialsError
import logging



logger = logging.getLogger()

logger.setLevel(logging.INFO)

def get_sns_error_topic_by_name(topic_name, boto_session):
    sns_client = boto_session.client('sns')
    response = sns_client.list_topics()
    topics = response['Topics']
    for topic in topics:
        if topic['TopicArn'].split(':')[-1] == topic_name:
            return topic['TopicArn']
    logger.error(f"Topic '{topic_name}' not found")
    raise Exception(f"Topic '{topic_name}' not found")

def send_sns_email(message, sns_topic_name, boto_session):
    """Send an email using SNS"""
    sns_topic_arn = get_sns_error_topic_by_name(sns_topic_name, boto_session)
    sns_client = boto_session.client('sns')
    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="Error Notification from API Request"
        )
        logger.warning(f"Email sent successfully via SNS {message}")
    except NoCredentialsError:
        logger.error("Credentials not available")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

