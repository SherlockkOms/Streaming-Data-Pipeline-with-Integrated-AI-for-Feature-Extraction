import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType, IntegerType, FloatType, ArrayType
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS property_stream WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    print("Keyspace created successfully")


def create_table(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS property_stream.property_details (
        title TEXT, 
        address TEXT, 
        link TEXT, 
        images LIST<TEXT>, 
        price TEXT, 
        bedrooms TEXT, 
        bathrooms TEXT, 
        description TEXT, 
        property_type TEXT, 
        receptions TEXT, 
        EPC_Rating TEXT, 
        tenure TEXT, 
        service_charge TEXT, 
        council_tax_band TEXT, 
        ground_rent TEXT
        PRIMARY KEY (link)
    )
    """
    session.execute(create_table_query)
    print("Table created successfully")


def cassandra_session():
    session = Cluster(['localhost']).connect('real_estate')
    if session is not None:
        create_keyspace(session)
        create_table(session)
    return session

def insert_data(session, **kwargs):
    print('Inserting data...')
    session.execute(
        """
        INSERT INTO property_stream.property_details (title, address, link, images, price, bedrooms, bathrooms, description, property_type, receptions, EPC_Rating, tenure, service_charge, council_tax_band, ground_rent)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,kwargs.values())
    print('Data inserted successfully')



def main():
    logging.basicConfig(level=logging.INFO)
    # Create a SparkSession
    spark = (SparkSession.builder.appName("RealEstateConsumer")
             .config("spark.cassandra.connection.host", "localhost")
             .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")
             .getOrCreate()
            )
    ## Reading streaming data from Kafka
    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "properties")
          .option("startingOffsets", "earliest")
          .load()
         )
    
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("address", StringType(), True),
        StructField("link", StringType(), True),
        StructField("images", ArrayType(StringType()), True),
        StructField("price", StringType(), True),
        StructField("bedrooms", StringType(), True),
        StructField("bathrooms", StringType(), True),
        StructField("description", StringType(), True),
        StructField("property_type", StringType(), True),
        StructField("receptions", StringType(), True),
        StructField("EPC Rating", StringType(), True),
        StructField("tenure", StringType(), True),
        StructField("service charge", StringType(), True),
        StructField("council tax band", StringType(), True),
        StructField("ground rent", StringType(), True)
    ])

    (df.selectExpr("CAST(value AS STRING) as value")
     .select(from_json(col("value"), schema).alias("data"))
     .select("data.*")
     )
    
    ## Writing the data to Cassandra
    cassandra_query = (df.writeStream
                       .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
                           lambda row: insert_data(cassandra_session(),**row.asDict())
                       ))
                       .start()  
                       .awaitTermination()
                        )

if __name__ == "__main__":
    main()