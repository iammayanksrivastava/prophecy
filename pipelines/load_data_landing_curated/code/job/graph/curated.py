from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def curated(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .save("dbfs:/mnt/curated/nyctaxi")
