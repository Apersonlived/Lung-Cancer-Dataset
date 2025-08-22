import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, explode

from pyspark.sql import types as T
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time
import time

def create_spark_session(spark_config, logger):
    """Initialize Spark Session."""
    return (SparkSession.builder.master(f"spark://{spark_config['master_ip']}:7077")
            .appName("SpotifyDataTransform")
            .config("spark.driver.memory", spark_config["driver_memory"])
            .config("spark.executor.memory", spark_config["executor_memory"])
            .config("spark.executor.cores", spark_config["executor_cores"])
            .config("spark.executor.instances", spark_config["executor_instances"])
            .getOrCreate()
    )

def load_and_clean(spark, input_dir, output_dir, logger):
    """Stage 1: Load data, drop duplicates, remove nulls, save cleaned data."""
    patient_schema= T.StructType([
        T.StructField("patient_id", T.StringType(), False),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("air_pollution", T.StringType(), True),
        T.StructField("alcohol_use", T.StringType(), True),
        T.StructField("dust_allergy", T.StringType(), True),
        T.StructField("occupational_hazards", T.StringType(), True),
        T.StructField("genetic_risk", T.StringType(), True),
        T.StructField("chronic_lung_disease", T.StringType(), True),
        T.StructField("balanced_diet", T.StringType(), True),
        T.StructField("obesity", T.StringType(), True),
        T.StructField("smoking", T.StringType(), True),
        T.StructField("passive_smoker", T.StringType(), True),
        T.StructField("chest_pain", T.StringType(), True),
        T.StructField("coughing_blood", T.StringType(), True),
        T.StructField("fatigue", T.StringType(), True),
        T.StructField("weight_loss", T.StringType(), True),
        T.StructField("shortness_of_breath", T.StringType(), True),
        T.StructField("wheezing", T.StringType(), True),
        T.StructField("swallowing_difficulty", T.StringType(), True),
        T.StructField("clubbing_finger_nails", T.StringType(), True),
    ])

    patient_df == spark.read.schema(patient_schema).csv(os.path.join(input_dir, "cancer patient datasets.csv"), header=True)
    patient_df = patient_df.dropDuplicates(["patient_id"]).filter(F.col("patient_id").isNotNull())
    patient_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "patients"))

    logger.info("Stage 1: Cleaned data saved")
    return patient_df
