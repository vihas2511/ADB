import logging
import sys
from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType
from pg_tw_fa_artemis.common import get_dbutils, get_spark


def insertCustHierarchyTask(spark, logger, rds_schema_name, target_db_name, target_table):
    logger.info("Running customer hierarchy query...")

    query = f"""
 #paste the query here
    """

    cust_df = spark.sql(query)

    # Rename columns to match target table schema
    cust_hierarchy656_na_lkp_df = cust_df.selectExpr(
        "gwscust as customer_id",
        "cust_1_id as l1_global_lvl",
        "cust_1_name as l1_global_name",
        "cust_2_id as l2_key_customer_group_lvl",
        "cust_2_name as l2_key_customer_group_name",
        "cust_3_id as l3_vary1_lvl",
        "cust_3_name as l3_vary1_name",
        "cust_4_id as l4_vary2_lvl",
        "cust_4_name as l4_vary2_name",
        "cust_5_id as l5_country_top_account_lvl",
        "cust_5_name as l5_country_top_account_name",
        "cust_6_id as l6_vary6_lvl",
        "cust_6_name as l6_vary6_name",
        "cust_7_id as l7_intrmdt_lvl",
        "cust_7_name as l7_intrmdt_name",
        "cust_8_id as l8_intrmdt2_lvl",
        "cust_8_name as l8_intrmdt2_name",
        "cust_9_id as l9_intrmdt3_lvl",
        "cust_9_name as l9_intrmdt3_name",
        "cust_10_id as l10_intrmdt4_lvl",
        "cust_10_name as l10_intrmdt4_name",
        "cust_11_id as l11_intrmdt5_lvl",
        "cust_11_name as l11_intrmdt5_name",
        "cust_12_id as l12_ship_to_lvl",
        "cust_12_name as l12_ship_to_name"
    )

    
    cust_hierarchy656_na_lkp_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_db_name}.{target_table}")

    logger.info(
        f"Data successfully written to {target_db_name}.{target_table}"
    )
    return 0


def main():
    spark = get_spark()
    dbutils = get_dbutils()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    #config = Configuration.load_for_default_environment(__file__, dbutils)

    rds_schema_name = f"{config['src-catalog-name']}.{config['rds-schema-name']}"
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config["tables"]["cust_hierarchy656_na_lkp"]

    logger.info("Source RDS schema: %s", rds_schema_name)
    logger.info("Target table: %s.%s", target_db_name, target_table)

    insertCustHierarchyTask(
        spark=spark,
        logger=logger,
        rds_schema_name=rds_schema_name,
        target_db_name=target_db_name,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
