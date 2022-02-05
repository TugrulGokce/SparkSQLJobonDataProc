import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", help="bucket for input and output")
args = parser.parse_args()

BUCKET = args.bucket
from pyspark.sql import SparkSession, SQLContext, Row
spark = SparkSession.builder.appName("DataExploring").getOrCreate()
sc = spark.sparkContext

data_file = "gs://{}/routes.dat".format(BUCKET)

routes = sc.textFile(data_file).cache()

routes.take(5)
routes_split = routes.map(lambda row : row.split(","))
parsed_routes = routes_split.map(lambda r : Row(
    airline = str(r[0]),
    airline_ID = r[1],
    source_airport = r[2],
    source_airport_ID = r[3],
    destination_airport = r[4],
    destination_airport_ID = r[5],
    codeshare = r[6],
    stops = r[7],
    equipment = r[8]
    )
)

parsed_routes.take(5)
# parse edilmis dat dosyasÄ±nÄ± dataframe'e cevirme 
sqlContext = SQLContext(sc)
routes_df = sqlContext.createDataFrame(parsed_routes)
routes_df.show(5)
routes_df.createTempView("routes")
query = spark.sql("""
              SELECT 
                  airline,
                  destination_airport,
                  COUNT(destination_airport) as Total_Destination_Flight
              FROM routes
              GROUP BY destination_airport, airline
              ORDER BY Total_Destination_Flight DESC
              LIMIT 20
              """)
query.show()
ax = query.sort("Total_Destination_Flight").toPandas().plot.barh(x='airline', figsize=(10,10))
ax.get_figure().savefig('report_routes.png');
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DateType


path = "gs://{}/whitehouse-waves-2014_03.csv".format(BUCKET)
    
white_house_visitor_record = spark.read.option("header", True).csv(path)
white_house_visitor_record = white_house_visitor_record.withColumn("Total_People", col("Total_People").cast(IntegerType())) \
                             .withColumn("APPT_MADE_DATE", col("APPT_MADE_DATE").cast(DateType())) \
                             .withColumn("APPT_START_DATE", col("APPT_START_DATE").cast(DateType())) \
                             .withColumn("APPT_END_DATE", col("APPT_END_DATE").cast(DateType())) \
                             .withColumn("APPT_CANCEL_DATE", col("APPT_CANCEL_DATE").cast(DateType())) \
                             .withColumn("LASTENTRYDATE", col("LASTENTRYDATE").cast(DateType())) \
                             .withColumn("RELEASE_DATE", col("RELEASE_DATE").cast(DateType()))

white_house_visitor_record.printSchema()
white_house_visitor_record.show(2)
white_house_visitor_record.createTempView("white_house_visitor")
white_house_query = spark.sql("""
                        SELECT 
                            visitee_namefirst ||' '|| visitee_namelast as visite_fullname,
                            COUNT(visitee_namefirst) as Total_Visit
                        FROM white_house_visitor
                        WHERE visitee_namelast != "null" and visitee_namefirst != 'VISITORS'
                        GROUP BY visite_fullname
                        ORDER BY Total_Visit DESC
                        LIMIT 20
                        """)
white_house_query.show()
ax = white_house_query.sort("Total_Visit").toPandas().plot.barh(x='visite_fullname', figsize=(10,10))
ax.get_figure().savefig('report_white_house_visitors.png');

import google.cloud.storage as gcs
bucket = gcs.Client().get_bucket(BUCKET)
for blob in bucket.list_blobs(prefix='sparksqlondataproc/'):
    blob.delete()

for fname in ['report_routes.png', 'report_white_house_visitors.png']:
    bucket.blob('sparksqlondataproc/{}'.format(fname)).upload_from_filename(fname)

bucket.blob('sparksqlondataproc/report_routes.png').upload_from_filename('report_routes.png')
bucket.blob('sparksqlondataproc/report_white_house_visitors.png').upload_from_filename('report_white_house_visitors.png')
