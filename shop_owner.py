try:
    import json
    from kafka import KafkaConsumer, KafkaProducer
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark.sql.functions import from_json, col, lit
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    from pymongo import MongoClient
    from cassandra.cluster import Cluster

    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'
except Exception as e:
    print("Error : {} ".format(e))

def shop_owner():
    ORDER_KAFKA_TOPIC = "order_details"
    ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

    def create_keyspace(session):   # key space for cassandra
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS florist_owner
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

        print("Keyspace created successfully!")


    def create_table(session):  # table for cassandra
        session.execute("""
        CREATE TABLE IF NOT EXISTS florist_owner.customer_order(
            id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            address TEXT,
            product TEXT,
            count INT,
            processed TEXT
            );
        """)

        print("Table created successfully!")


    def create_cassandra_connection():  # initialize cassandra
        try:
            # connecting to the cassandra cluster
            cluster = Cluster(['localhost'])

            cas_session = cluster.connect()

            return cas_session
        except Exception as e:
            return None

    ################# Kafka #################
    consumer = KafkaConsumer(
        ORDER_KAFKA_TOPIC,
        bootstrap_servers="localhost:29092"
    )
    producer = KafkaProducer(bootstrap_servers="localhost:29092")

    ################# Spark #################
    spark = SparkSession.builder \
        .master("local") \
        .appName("owner-log") \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
        .config('spark.cassandra.connection.host', 'localhost') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    ################# Kafka + Spark Stream #################
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe",ORDER_KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("includeHeaders", "true") \
        .option("failOnDataLoss", "false") \
        .load()

    schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("address", StringType(), False),
            StructField("product", StringType(), False),
            StructField("count", IntegerType(), False),
            StructField("processed", StringType(), False),
        ])

    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")

    session = create_cassandra_connection()
    create_keyspace(session)
    create_table(session)

    kafka_df = kafka_df.withColumn("processed",lit(str("True")))
    streaming_query = (kafka_df.writeStream.format("org.apache.spark.sql.cassandra")\
                                .option('checkpointLocation', '/tmp/checkpoint')\
                                .option('keyspace', 'florist_owner')\
                                .option('table', 'customer_order')\
                                .trigger(once=True)\
                                .start())
    print("adding")
    streaming_query.awaitTermination()

    rows = session.execute('select * from florist_owner.customer_order limit 5;')
    for row in rows:
        print(row)

    print("Gonna start listening")

    for message in consumer:
        print("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        data = "Your order is confirmed"
        print("Successful transaction..")
        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode('utf-8'))

    spark.stop()