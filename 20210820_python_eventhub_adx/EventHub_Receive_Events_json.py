# Show Azure subscription information
import os
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import time
import asyncio

#variables to build connection
ev_connstr = ""
ev_name = ""

sa_connstr = ""
sa_name = ""
sa_containername = ""

async def on_event(partition_context, event):
    print("{}".format(event.body_as_json(encoding='UTF-8')))
    print({k.decode("utf-8"):v.decode("utf-8") for k,v in event.properties.items()})
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(conn_str=sa_connstr, container_name=sa_containername)

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(conn_str=ev_connstr, consumer_group="$Default", eventhub_name=ev_name, checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event, track_last_enqueued_event_properties=True,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())