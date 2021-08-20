# for git - https://gist.github.com/mindplace/b4b094157d7a3be6afd2c96370d39fad

from pyspark.sql import SparkSession
import configparser
from src.main.python.gkfunctions import read_schema

from datetime import datetime, timedelta, time
from pyspark.sql import functions as psf

# Initializing SparkSession
spark = SparkSession.builder.appName("DataIngestionAndRefine").master("local[*]").getOrCreate()

# Reading the configs
config = configparser.ConfigParser()
config.read(r'../projconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaFromConfig = config.get('schema', 'landingFileSchema')

# handling Dates

today = datetime.now()
yesterdayDate = today - timedelta(1)
# currentDaySuffix = "_" + today.strftime("%d%m%Y")
# previousDaySuffix = "_" + yesterdayDate.strftime("%d%m%Y")
currentDaySuffix = "_05062020"
previousDaySuffix = "_04062020"
# input date format _ddmmyyyy

# Reading from landing Zone
fileSchema = read_schema(landingFileSchemaFromConfig)
landingFileDF = spark.read.schema(fileSchema) \
    .option("delimiter", "|") \
    .csv(inputLocation + "Sales_Landing\\SalesDump" + currentDaySuffix)

# landingFileDF.show()

# Reading the previous day held data
previousHeldDF = spark.read.schema(fileSchema) \
    .option("delimiter", "|") \
    .option("header", False) \
    .csv(outputLocation + "Hold\\HoldData" + previousDaySuffix)

landingFileDF.createOrReplaceTempView("landingFileDFView")
previousHeldDF.createOrReplaceTempView("previousHeldDFView")

# if today's data contain yesterday's held saleid , take the today's data for that saleid.
# but if today's data contain null for the saleid check if yesterday data had value if yes, consider that value.
refreshedDF = spark.sql("select a.Sale_ID, a.Product_ID, "
                        "case "
                        "when a.Quantity_Sold is null then b.Quantity_Sold else a.Quantity_Sold "
                        "end as Quantity_Sold, "
                        "case "
                        "when a.Vendor_ID is null then b.Vendor_ID else a.Vendor_ID "
                        "end as Vendor_ID, "
                        "a.Sale_Date, a.Sale_Amount, a.Sale_Currency "
                        "from landingFileDFView a left outer join previousHeldDFView b "
                        "on a.Sale_ID = b.Sale_ID")

# refreshedDF.show()
validLandingDF = refreshedDF.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_ID").isNotNull())

# for invalid need to consider yesterday's held that was not picked up by refreshedDF (i.e., not picked up in the
# step above)

releasedFromHeldDF = spark.sql("select a.Sale_ID "
                               "from landingFileDFView a inner join previousHeldDFView b "
                               "on a.Sale_ID = b.Sale_ID")

releasedFromHeldDF.createOrReplaceTempView("releasedFromHeldDFView")
notReleasedFromHeldDF = spark.sql("select * "
                                  "from previousHeldDFView "
                                  "where Sale_ID not in (select Sale_ID from releasedFromHeldDFView)")
# notReleasedFromHeldDF.show()

invalidLandingDF = refreshedDF.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull())\
    .union(notReleasedFromHeldDF)
# invalidLandingDF.show()

validLandingDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Valid\\ValidData" + currentDaySuffix)

# invalidLandingDF.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header", True)\
#     .csv(outputLocation + "Hold\\HoldData" + currentDaySuffix)
