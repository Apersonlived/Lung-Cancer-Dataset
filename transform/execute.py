import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time
import time


def create_spark_session(logger):
    """Initialize Spark Session."""
    return (SparkSession.builder
            .appName("LungCancerDataTransform")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
    )


def load_and_clean(spark, input_dir, output_dir, logger):
    """Stage 1: Load data, drop duplicates, remove nulls, save cleaned data."""
    input_file = os.path.join(input_dir, "cancer patient data sets.csv")

    # Step 1: Load raw CSV with headers
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Step 2: Standardize column names (lowercase + snake_case)
    rename_map = {
        "Patient Id": "patient_id",
        "Age": "age",
        "Gender": "gender",
        "Air Pollution": "air_pollution",
        "Alcohol use": "alcohol_use",
        "Dust Allergy": "dust_allergy",
        "OccuPational Hazards": "occupational_hazards",
        "Genetic Risk": "genetic_risk",
        "chronic Lung Disease": "chronic_lung_disease",
        "Balanced Diet": "balanced_diet",
        "Obesity": "obesity",
        "Smoking": "smoking",
        "Passive Smoker": "passive_smoker",
        "Chest Pain": "chest_pain",
        "Coughing of Blood": "coughing_blood",
        "Fatigue": "fatigue",
        "Weight Loss": "weight_loss",
        "Shortness of Breath": "shortness_of_breath",
        "Wheezing": "wheezing",
        "Swallowing Difficulty": "swallowing_difficulty",
        "Clubbing of Finger Nails": "clubbing_finger_nails",
        "Frequent Cold": "frequent_cold",
        "Dry Cough": "dry_cough",
        "Snoring": "snoring",
        "Level": "level"
    }

    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    # Drop index column if it exists
    if "index" in df.columns:
        df = df.drop("index")

    # Step 3: Remove duplicate & null patient_id
    df = df.dropDuplicates(["patient_id"]).filter(F.col("patient_id").isNotNull())

    # Save cleaned dataset
    df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "patients"))
    logger.info("Stage 1: Cleaned data saved")

    return df


def create_table(output_dir, patient_df, logger):
    """Stage 2: Create analytical table for further ML/BI usage."""
    # Select features for modeling
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

    if len(sys.argv) != 3:
        logger.info("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    logger.info("Starting transform stage")
    start = time.time()

    spark = create_spark_session(logger)

    patient_df = load_and_clean(spark, input_dir, output_dir, logger)
    create_table(output_dir, patient_df, logger)

    end = time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken {format_time(end-start)}")
