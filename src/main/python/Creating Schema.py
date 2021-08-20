from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# Initializing SparkSession
spark = SparkSession.builder.appName("DataIngestionAndRefine").master("local[*]").getOrCreate()

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
    .csv("C:\\Users\\Jaya\\PycharmProjects\\GKC_StoresPipeline\\Data\\Inputs\\Sales_Landing\\SalesDump_04062020")

landingFileDF.show()
