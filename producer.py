try:
    import json
    import time
    from kafka import KafkaProducer
except Exception as e:
    print("Error : {} ".format(e))

def producer():
    ORDER_KAFKA_TOPIC = "order_details"
    ORDER_LIMIT = 10

    producer = KafkaProducer(bootstrap_servers="localhost:29092")

    print("Going to be generating order after 10 seconds")
    print("Will generate one unique order every 5 seconds")
    time.sleep(10)

    for i in range(ORDER_LIMIT):
        data = {
            "id": i,
            "first_name": f"tom_{i}",
            "last_name": f"doe{i}",
            "address": "street",
            "product": "tulip",
            "count": i,
        }

        producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
        print(f"Done Sending..{i}")
        time.sleep(2)
