# Show Azure subscription information
import os
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import time
import asyncio
import json
from datetime import datetime
import random
import sched


#variables to build connection
ev_connstr = ""
ev_name = ""

async def create_batch(producer):
    event_data_batch = await producer.create_batch()
    i = 0
    while i <= 1000:
        device_id = random.randint(0,2)
        json_obj = {
            "timestamp": str(datetime.utcnow()),
            "temperature": round(random.uniform(16.8,37.5),3),
            "humidity": round(random.uniform(40.0,62.4),2),
            "iotdevice": "device"+str(device_id)
            }
        string = json.dumps(json_obj)
        Event_data = EventData(body=string)
        Event_data.properties = {
            "Table":"TestTable",
            "IngestionMappingReference":"TestMapping", 
            "Format":"json"
            }
        print(Event_data)
        event_data_batch.add(Event_data)
        i += 1
    print(event_data_batch)
    return event_data_batch


async def send_batch():
    producer = EventHubProducerClient.from_connection_string(conn_str=ev_connstr, eventhub_name=ev_name)
    batch_data = await create_batch(producer)
    async with producer:
        await producer.send_batch(batch_data)


s = sched.scheduler(time.time, time.sleep)
def do_something(sc):
    print("events send at:"+str(datetime.now()))
    asyncio.run(send_batch())
    s.enter(1, 1, do_something, (sc,))

s.enter(1, 1, do_something, (s,))
s.run()




