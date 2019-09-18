import pulsar

def Consumer():
    client = pulsar.Client('pulsar://pulsar.rc.com:6650')
    consumer = client.subscribe('persistent://public/xxxxx/xxxxx', 'yyyyyy',consumer_type=pulsar.ConsumerType.Shared)
    while True:
        msg = consumer.receive()
        try:
            print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
            consumer.acknowledge(msg)
        except:
             consumer.negative_acknowledge(msg)
        client.close()
if __name__ == "__main__":
    Consumer()