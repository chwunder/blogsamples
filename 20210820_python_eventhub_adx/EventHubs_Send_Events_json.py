# Show Azure subscription information
import os
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import time
import asyncio
import json
from datetime import datetime
import random

#variables to build connection
ev_connstr = "Endpoint=sb://ehn1-chwunder.servicebus.windows.net/;SharedAccessKeyName=srm;SharedAccessKey=VvARWbTrmbexaGE194Ggc1783OExsgY/A19LdoBe8uA=;EntityPath=ehi1-chwunder"
ev_name = "ehi1-chwunder"

async def create_batch(producer):
    event_data_batch = await producer.create_batch()
    i = 0
    while i <= 10:
        device_id = random.randint(0,2)
        json_obj = {"timestamp": str(datetime.utcnow()),"temperature": round(random.uniform(16.8,37.5),3), "iotdevice": "device"+str(device_id)}
        string = json.dumps(json_obj)
        Event_data = EventData(body=string)
        Event_data.properties = {"table":"TestTable", "ingestionMappingReference":"TestMapping", "format":"json"}
        event_data_batch.add(Event_data)
        i += 1
    print(event_data_batch)
    return event_data_batch


async def send_batch():
    producer = EventHubProducerClient.from_connection_string(conn_str=ev_connstr, eventhub_name=ev_name)
    batch_data = await create_batch(producer)
    async with producer:
        await producer.send_batch(batch_data)

asyncio.run(send_batch())