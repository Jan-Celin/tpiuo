import asyncio
import json

import requests

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

subreddit = 'dataengineering'
limit = 10
timeframe = 'all'
listing = 'top'


def get_reddit(subreddit, listing, limit, timeframe):
    try:
        base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
        request = requests.get(base_url, headers={'User-agent': 'yourbot'})
    except:
        print('An Error Occured')
    return request.json()


r = get_reddit(subreddit, listing, limit, timeframe)
print(r)

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://lab1namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eT9XIw8LjKukjIcS17UhYl7dyteynEdFB+AEhE/L3e4="
EVENT_HUB_NAME = "lab1eventhub"

async def run(r):
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData(json.dumps(r)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

asyncio.run(run(r))

print("Sent!")

while True:
    a = 1