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
        T.StructField("gender", T.IntegerType(), True),
        T.StructField("air_pollution", T.IntegerType(), True),
        T.StructField("alcohol_use", T.IntegerType(), True),
        T.StructField("dust_allergy", T.IntegerType(), True),
        T.StructField("occupational_hazards", T.IntegerType(), True),
        T.StructField("genetic_risk", T.IntegerType(), True),
        T.StructField("chronic_lung_disease", T.IntegerType(), True),
        T.StructField("balanced_diet", T.IntegerType(), True),
        T.StructField("obesity", T.IntegerType(), True),
        T.StructField("smoking", T.IntegerType(), True),
        T.StructField("passive_smoker", T.IntegerType(), True),
        T.StructField("chest_pain", T.IntegerType(), True),
        T.StructField("coughing_blood", T.IntegerType(), True),
        T.StructField("fatigue", T.IntegerType(), True),
        T.StructField("weight_loss", T.IntegerType(), True),
        T.StructField("shortness_of_breath", T.IntegerType(), True),
        T.StructField("wheezing", T.IntegerType(), True),
        T.StructField("swallowing_difficulty", T.IntegerType(), True),
        T.StructField("clubbing_finger_nails", T.IntegerType(), True),
    ])

    patient_df = spark.read.schema(patient_schema).csv(os.path.join(input_dir, "cancer patient datasets.csv"), header=True)
    patient_df = patient_df.dropDuplicates(["patient_id"]).filter(F.col("patient_id").isNotNull())
    patient_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "patients"))

    logger.info("Stage 1: Cleaned data saved")
    return patient_df

def create_table(output_dir, patient_df, logger):
    """Stage 2: Create analytical table for further ML/BI usage."""
    # Select features of interest for modeling
    patient_df = patient_df.select(
        "patient_id", "age", "gender", "smoking", "alcohol_use", "genetic_risk",
        "chronic_lung_disease", "chest_pain", "shortness_of_breath",
        "weight_loss", "air_pollution", "obesity"
    )

    patient_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "patients_features"))
    logger.info("Stage 2: Analytical features table saved")
    return patient_df


if __name__ == "__main__":
    logger = setup_logging("transform.log")

    if len(sys.argv) != 8:
        logger.info("Usage: python script.py <input_dir> <output_dir> master_ip driver memory executor memory...")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    spark_config = {}
    spark_config["master_ip"] = sys.argv[3]
    spark_config["driver_memory"] = sys.argv[4]
    spark_config["executor_memory"] = sys.argv[5]
    spark_config["executor_cores"] = sys.argv[6]
    spark_config["executor_instances"] = sys.argv[7]


    logger.info("Starting transform stage")
    start = time.time()
    spark = create_spark_session(spark_config, logger)

    patient_df = load_and_clean(spark, input_dir, output_dir, logger)
    create_table(output_dir, patient_df, logger)

    end =time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken {format_time(end-start)}")