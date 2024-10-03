import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, from_json

def create_keyspace(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS property_streams
    WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS property_streams.properties (
        price TEXT,
        title TEXT,
        link TEXT,
        pictures TEXT[],
        floor_plan TEXT,
        address TEXT,
        bedrooms TEXT,
        bathrooms TEXT,
        receptions TEXT,
        epc_rating TEXT,
        tenure TEXT,
        time_remaining_on_lease TEXT,
        service_charge TEXT,
        council_tax_band TEXT,
        ground_rent TEXT,
        PRIMARY KEY (link)
    );
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("Inserting data...")
    session.execute("""
    INSERT INTO property_streams.properties(price, title, link, pictures, floor_plan, 
                                             address, bedrooms, bathrooms, receptions, 
                                             epc_rating, tenure, time_remaining_on_lease, 
                                             service_charge, council_tax_band, ground_rent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, kwargs.values())
    print("Data inserted successfully!")

def create_cassandra_session():
    session = Cluster(["localhost"]).connect()
    if session is not None:
        create_keyspace(session)
        create_table(session)
    return session

def process_batch(batch_df, session):
    for row in batch_df.collect():
        insert_data(session, **row.asDict())

def main():
    logging.basicConfig(level=logging.INFO)

    # Create Cassandra session once
    cassandra_session = create_cassandra_session()

    # Create Spark session
    spark = (SparkSession.builder.appName("RealEstateConsumer")
             .config("spark.cassandra.connection.host", "localhost")
             .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.2,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2")
             .getOrCreate()
             )

    # Kafka Stream configuration
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")  # Fixed typo
                .option("subscribe", "properties")
                .option("startingOffsets", "earliest")
                .load()
                )

    # Define schema for incoming data
    schema = StructType([
        StructField("price", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("pictures", ArrayType(StringType()), nullable=True),
        StructField("floor_plan", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("bedrooms", StringType(), nullable=True),
        StructField("bathrooms", StringType(), nullable=True),
        StructField("receptions", StringType(), nullable=True),
        StructField("epc_rating", StringType(), nullable=True),
        StructField("tenure", StringType(), nullable=True),
        StructField("time_remaining_on_lease", StringType(), nullable=True),
        StructField("service_charge", StringType(), nullable=True),
        StructField("council_tax_band", StringType(), nullable=True),
        StructField("ground_rent", StringType(), nullable=True)
    ])

    # Parse the Kafka value into a structured format
    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                )

    # WriteStream with foreachBatch to insert into Cassandra
    cassandra_query = (kafka_df.writeStream
                       .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, cassandra_session))
                       .start()
                       .awaitTermination()
                       )

if __name__ == "__main__":
    main()
