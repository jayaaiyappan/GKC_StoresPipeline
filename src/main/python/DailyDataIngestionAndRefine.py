# for git - https://gist.github.com/mindplace/b4b094157d7a3be6afd2c96370d39fad

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import configparser

# Initializing SparkSession
spark = SparkSession.builder.appName("DataIngestionAndRefine").master("local[*]").getOrCreate()

# Reading the configs
config = configparser.ConfigParser()
config.read(r'../projconfigs/config.ini')
inputlocation = config.get('paths', 'inputlocation')

# Reading from landing Zone

landingFileSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', IntegerType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', TimestampType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])

landingFileDF = spark.read.schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv(inputlocation)

landingFileDF.show()
