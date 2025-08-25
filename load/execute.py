import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

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

def create_postgres_tables(logger, pg_un, pg_pw, pg_p, pg_h, pg_db):
    """Create PostgreSQL tables for cancer dataset if they don't exist."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_un,
            password=pg_pw,
            host=pg_h,
            port=pg_p
        )
        cursor = conn.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS patients (
                patient_id VARCHAR(50) PRIMARY KEY,
                age INTEGER,
                gender INTEGER,
                air_pollution INTEGER,
                alcohol_use INTEGER,
                dust_allergy INTEGER,
                occupational_hazards INTEGER,
                genetic_risk INTEGER,
                chronic_lung_disease INTEGER,
                balanced_diet INTEGER,
                obesity INTEGER,
                smoking INTEGER,
                passive_smoker INTEGER,
                chest_pain INTEGER,
                coughing_blood INTEGER,
                fatigue INTEGER,
                weight_loss INTEGER,
                shortness_of_breath INTEGER,
                wheezing INTEGER,
                swallowing_difficulty INTEGER,
                clubbing_finger_nails INTEGER,
                frequent_cold INTEGER,
                dry_cough INTEGER,
                snoring INTEGER,
                level TEXT CHECK(level IN ('Low', 'Medium', 'High'))
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS patients_features (
                patient_id VARCHAR(50) PRIMARY KEY,
                age INTEGER,
                gender INTEGER,
                smoking INTEGER,
                alcohol_use INTEGER,
                genetic_risk INTEGER,
                chronic_lung_disease INTEGER,
                chest_pain INTEGER,
                shortness_of_breath INTEGER,
                weight_loss INTEGER,
                air_pollution INTEGER,
                obesity INTEGER
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.warning(f"Error creating tables: {e}")
    finally:
        logger.debug("Closing database connection and cursor to postgres")
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(spark, input_dir, pg_un, pg_pw):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{pg_h}:{pg_p}/{pg_db}"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage1/patients", "patients"),
        ("stage2/patients_features", "patients_features"),
    ]

    for parquet_path, table_name in tables:
        try:
            full_path = os.path.join(input_dir, parquet_path)
            logger.info(f"Reading from: {full_path}")
            df = spark.read.parquet(full_path)
            mode = "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            logger.info(f"Loaded {table_name} to PostgreSQL.")
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")


if __name__ == "__main__":
    logger = setup_logging("load.log")

    if len(sys.argv) != 7:
        logger.error("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw> <pg_h> <pg_db> <pg_p>")
        sys.exit(1)

    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]
    pg_h = sys.argv[4]
    pg_db = sys.argv[5]
    pg_p = sys.argv[6]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist.")
        sys.exit(1)

    logger.info("Load stage has started")
    start = time.time()

    spark = create_spark_session(logger)
    create_postgres_tables(logger, pg_un, pg_pw, pg_p, pg_h, pg_db)
    load_to_postgres(spark, input_dir, pg_un, pg_pw)

    end = time.time()
    logger.info("Load stage completed")
    logger.info(f"Total time taken {format_time(end-start)}")