from kafka import KafkaConsumer

def customer():
    consumer = KafkaConsumer('order_confirmed', bootstrap_servers='localhost:29092')

    print("Gonna start listening")
    while True:
        for message in consumer:
            print("Here is a message..")
            print (message)