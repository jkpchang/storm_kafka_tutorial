#!/usr/bin/env python

from kafka import KafkaConsumer, KafkaClient, MultiProcessConsumer

kafka = KafkaClient("hadoop-m.c.onefold-1.internal:6667")

# To consume messages
consumer = MultiProcessConsumer(kafka, "my-group", "truckevent2", num_procs=1)
# consumer = KafkaConsumer("truckevent2",
#                          group_id="my_group",
#                          bootstrap_servers=["130.211.146.208:6667"])

for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))

kafka.close()
