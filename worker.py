"""
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import division

import base64
import json
import time, datetime
import requests
import click

from google.cloud import pubsub
from google.cloud import storage

from logger import Logger
from recurror import Recurror
from mediator import Mediator

METADATA_URL_PROJECT = "http://metadata/computeMetadata/v1/project/"
METADATA_URL_INSTANCE = "http://metadata/computeMetadata/v1/instance/"
METADTA_FLAVOR = {'Metadata-Flavor' : 'Google'}

# Get the metadata related to the instance using the metadata server
PROJECT_ID = requests.get(METADATA_URL_PROJECT + 'project-id', headers=METADTA_FLAVOR).text
INSTANCE_ID = requests.get(METADATA_URL_INSTANCE + 'id', headers=METADTA_FLAVOR).text
INSTANCE_NAME = requests.get(METADATA_URL_INSTANCE + 'hostname', headers=METADTA_FLAVOR).text
INSTANCE_ZONE_URL = requests.get(METADATA_URL_INSTANCE + 'zone', headers=METADTA_FLAVOR).text
INSTANCE_ZONE = INSTANCE_ZONE_URL.split('/')[0]

# Parameters to call with the script
@click.command()
@click.option('--toprocess', default=1,
              help='Number of medias to process on one instance at a time - Not implemented')
@click.option('--subscription', required=True, help='Name of the subscription to get new messages')
@click.option('--topic', required=True, help='Name of the topic to get new messages')
@click.option('--refresh', default=25, help='Acknowledge deadline refresh time')
@click.option('--dataset_id', default='media_processing', help='Name of the dataset where to save transcript')
@click.option('--table_id', default='speech', help='Name of the table where to save transcript')

def main(toprocess, subscription, topic, refresh, dataset_id, table_id):
    """
    """
    # temporary forcing of PROJECT ID to avoid issues with getting metadata for tests
    PROJECT_ID = "wired-height-198314"
    subscription_id = "projects/{0}/subscriptions/{1}".format(PROJECT_ID, subscription)
    topic_name = "projects/{0}/topics/{1}".format(PROJECT_ID, topic)
    
    # subscription = pubsub.subscription.Subscription(subscription_id, client=pubsub_client)
    # subscription already exists, created in the console
    # pubsub_client.create_subscription(subscription_id,topic_name)
    # unfortunate naming of two variables with the same name... 
    subscription = pubsub_client.subscribe(subscription_id)

    """if not subscription.exists():
        sys.stderr.write('Cannot find subscription {0}\n'.format(sys.argv[1]))
        return
    
    #r = Recurror(refresh - 10, postpone_ack)
    """ 


    # Define the callback.
    # Note that the callback is defined *before* the subscription is opened.
    def callback(message):
        # pull() blocks until a message is received
        data = message.data
        msg_string = base64.b64decode(data)
        msg_data = json.loads(msg_string)
        content_type = msg_data["contentType"]

        attributes = message.attributes
        event_type = attributes['eventType']
        bucket_id = attributes['bucketId']
        object_id = attributes['objectId']
        generation = attributes['objectGeneration']
        #[END msg_format]

        Logger.log_writer("{0} process starts".format(object_id))
        start_process = datetime.datetime.now()

        # <Your custom process>
        if event_type == 'OBJECT_FINALIZE':
            m = Mediator(bucket_id, object_id, content_type, PROJECT_ID, dataset_id, table_id)
            m.speech_to_text()
        # <End of your custom process>

        end_process = datetime.datetime.now()
        Logger.log_writer("{0} process stops".format(object_id))

        # Write logs only if needed for analytics or debugging
        Logger.log_writer(
            "{media_url} processed by instance {instance_hostname} in {amount_time}"
            .format(
                media_url=msg_string,
                instance_hostname=INSTANCE_NAME,
                amount_time=str(end_process - start_process)
            )
        )

        message.ack()
        
    # Open the subscription, passing the callback.
    future = subscription.open(callback)
    
    
def postpone_ack(params):
    """Postpone the acknowledge deadline until the media is processed
    Will be paused once a message is processed until a new one arrives
    Args:
        ack_ids: List of the message ids in the queue
    Returns:
        None
    Raises:
        None
    """
    ack_ids = params['ack_ids']
    refresh = params['refresh']
    sub = params['sub']
    Logger.log_writer(','.join(ack_ids) + ' postponed')

    #[START postpone_ack]
    #Increment the ackDeadLine to make sure that file has time to be processed
    pubsub_client.projects().subscriptions().modifyAckDeadline(
        subscription=sub,
        body={
            'ackIds': ack_ids,
            'ackDeadlineSeconds': refresh
        }
    ).execute()
    #[END postpone_ack]

"""Create the API clients."""
pubsub_client = pubsub.SubscriberClient()
gcs_client = storage.Client()

"""Launch the loop to pull media to process."""
if __name__ == '__main__':
    main()
