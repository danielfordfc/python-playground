from confluent_kafka import Producer
bootstrap_servers = "localhost:9092"
transactional_id = "trans-id"
topic = "test-transaction"
producer_config = {
    "bootstrap.servers": bootstrap_servers,
    # 'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
    # 'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
    'transactional.id': transactional_id
}
producer = Producer(producer_config)
producer.init_transactions()
producer.begin_transaction()
def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
try:
    for i in range(1, 3):
        producer.produce(topic, key=f'key{i}', value=f'value{i}', callback=delivery_report)
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    raise e
producer.flush()