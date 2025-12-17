from confluent_kafka import Consumer

conf_consumer = {
    "bootstrap.servers": "server.crexdata.eu:9192",
    "message.max.bytes": 104857600,  # 100 MiB
    "socket.timeout.ms": 30000,  # 30sec
    "group.id": "odm-kafka-bridge",
    "auto.offset.reset": "earliest",  # read msgs from before subscription too
    "enable.partition.eof": True,  # can be used to detect end of stream
}
c = Consumer(conf_consumer)

c.subscribe(["UPB-CREXDATA-RealtimeDEM-Stream"])

print("Waiting for messages...")
while True:
    msg = c.poll(2.0)
    if msg is None:
        print("No message...")
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    print(f"Key: {msg.key()}")
    print(f"Headers: {msg.headers()}")
    print(f"Offset: {msg.offset()} Partition: {msg.partition()}")
