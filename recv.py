import logging
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventData


EVENT_HUB_CONNECTION_STR = "Endpoint=sb://lab1namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eT9XIw8LjKukjIcS17UhYl7dyteynEdFB+AEhE/L3e4="
EVENT_HUB_NAME = "lab1eventhub"

async def on_event(partition_context, event):
    print("Received event from partition {} with data: {}".format(partition_context.partition_id, event.body_as_str()))
    await partition_context.update_checkpoint(event)

async def receive():
    client = EventHubConsumerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STR, consumer_group="$Default", eventhub_name=EVENT_HUB_NAME)
    async with client:
        await client.receive(on_event=on_event, starting_position="@latest")

# run the receive function in an asyncio loop
loop = asyncio.get_event_loop()
loop.run_until_complete(receive())