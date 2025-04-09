import logging
import sys
#Need to change the config file 
from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import SparkSession
from pg_tw_fa_artemis.common import get_dbutils, get_spark

def insertCustHierarchyTask(spark, logger, g11_db_name, target_db_name, target_table):
    # SQL query to be executed
    query = f"""
    INSERT OVERWRITE TABLE {target_db_name}.cust_hierarchy656_na_lkp
    SELECT 
        CASE 
            WHEN ch.cust_orig_lvl = 1  THEN ch.cust_1_id
            WHEN ch.cust_orig_lvl = 2  THEN ch.cust_2_id
            WHEN ch.cust_orig_lvl = 3  THEN ch.cust_3_id
            WHEN ch.cust_orig_lvl = 4  THEN ch.cust_4_id
            WHEN ch.cust_orig_lvl = 5  THEN ch.cust_5_id
            WHEN ch.cust_orig_lvl = 6  THEN ch.cust_6_id
            WHEN ch.cust_orig_lvl = 7  THEN ch.cust_7_id
            WHEN ch.cust_orig_lvl = 8  THEN ch.cust_8_id
            WHEN ch.cust_orig_lvl = 9  THEN ch.cust_9_id
            WHEN ch.cust_orig_lvl = 10 THEN ch.cust_10_id
            WHEN ch.cust_orig_lvl = 11 THEN ch.cust_11_id
            WHEN ch.cust_orig_lvl = 12 THEN ch.cust_12_id
            ELSE ''
        END AS gwscust,
        ch.cust_1_id,
        ch.cust_1_name,
        ch.cust_2_id,
        ch.cust_2_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 3 
                THEN sub.new_group
            WHEN ch.cust_orig_lvl >= 3 
                THEN ch.cust_3_id
            ELSE ''
        END AS cust_3_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 3 
                THEN sub.new_group
            WHEN ch.cust_orig_lvl >= 3 
                THEN ch.cust_3_name
            ELSE ''
        END AS cust_3_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 4 
                THEN ch.cust_3_id
            WHEN ch.cust_orig_lvl >= 4 
                THEN ch.cust_4_id
            ELSE ''
        END AS cust_4_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 4 
                THEN ch.cust_3_name
            WHEN ch.cust_orig_lvl >= 4 
                THEN ch.cust_4_name
            ELSE ''
        END AS cust_4_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 5 
                THEN ch.cust_4_id
            WHEN ch.cust_orig_lvl >= 5 
                THEN ch.cust_5_id
            ELSE ''
        END AS cust_5_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 5 
                THEN ch.cust_4_name
            WHEN ch.cust_orig_lvl >= 5 
                THEN ch.cust_5_name
            ELSE ''
        END AS cust_5_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 6 
                THEN ch.cust_5_id
            WHEN ch.cust_orig_lvl >= 6 
                THEN ch.cust_6_id
            ELSE ''
        END AS cust_6_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 6 
                THEN ch.cust_5_name
            WHEN ch.cust_orig_lvl >= 6 
                THEN ch.cust_6_name
            ELSE ''
        END AS cust_6_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 7 
                THEN ch.cust_6_id
            WHEN ch.cust_orig_lvl >= 7 
                THEN ch.cust_7_id
            ELSE ''
        END AS cust_7_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 7 
                THEN ch.cust_6_name
            WHEN ch.cust_orig_lvl >= 7 
                THEN ch.cust_7_name
            ELSE ''
        END AS cust_7_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 8 
                THEN ch.cust_7_id
            WHEN ch.cust_orig_lvl >= 8 
                THEN ch.cust_8_id
            ELSE ''
        END AS cust_8_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 8 
                THEN ch.cust_7_name
            WHEN ch.cust_orig_lvl >= 8 
                THEN ch.cust_8_name
            ELSE ''
        END AS cust_8_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 9 
                THEN ch.cust_8_id
            WHEN ch.cust_orig_lvl >= 9 
                THEN ch.cust_9_id
            ELSE ''
        END AS cust_9_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 9 
                THEN ch.cust_8_name
            WHEN ch.cust_orig_lvl >= 9 
                THEN ch.cust_9_name
            ELSE ''
        END AS cust_9_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 10 
                THEN ch.cust_9_id
            WHEN ch.cust_orig_lvl >= 10 
                THEN ch.cust_10_id
            ELSE ''
        END AS cust_10_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 10 
                THEN ch.cust_9_name
            WHEN ch.cust_orig_lvl >= 10 
                THEN ch.cust_10_name
            ELSE ''
        END AS cust_10_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 11 
                THEN ch.cust_10_id
            WHEN ch.cust_orig_lvl >= 11 
                THEN ch.cust_11_id
            ELSE ''
        END AS cust_11_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 11 
                THEN ch.cust_10_name
            WHEN ch.cust_orig_lvl >= 11 
                THEN ch.cust_11_name
            ELSE ''
        END AS cust_11_name,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 12 
                THEN ch.cust_11_id
            WHEN ch.cust_orig_lvl >= 12 
                THEN ch.cust_12_id
            ELSE ''
        END AS cust_12_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 12 
                THEN ch.cust_11_name
            WHEN ch.cust_orig_lvl >= 12 
                THEN ch.cust_12_name
            ELSE ''
        END AS cust_12_name
    FROM (
        SELECT 
            cust_1_id,
            cust_1_name,
            cust_2_id, 
            cust_2_name,
            cust_3_id, 
            cust_3_name,
            cust_4_id, 
            cust_4_name,
            cust_5_id, 
            cust_5_name,
            cust_6_id, 
            cust_6_name,
            cust_7_id, 
            cust_7_name,
            cust_8_id, 
            cust_8_name,
            cust_9_id, 
            cust_9_name,
            cust_10_id, 
            cust_10_name,
            cust_11_id, 
            cust_11_name,
            cust_12_id,
            cust_12_name,
            CASE 
                WHEN cust_2_id IN ('9900000002', '9900000007') 
                    THEN cust_orig_lvl + 1
                ELSE cust_orig_lvl
            END AS cust_orig_lvl
        FROM {g11_db_name}.cust_hier_dim
        WHERE cust_hier_id = '656'
            AND curr_ind = 'Y'
            AND cust_1_id = '9900000001'
    ) ch
    LEFT JOIN (
        SELECT 
            b.cust_2_id, 
            b.cust_3_id,
            b.rn,
            CASE 
                WHEN b.rn < 5001
                    THEN CONCAT(cust_2_id, '-LIST-01')
                WHEN b.rn BETWEEN 5001 AND 10000 
                    THEN CONCAT(cust_2_id, '-LIST-02')
                WHEN b.rn BETWEEN 10001 AND 15000 
                    THEN CONCAT(cust_2_id, '-LIST-03')
                WHEN b.rn BETWEEN 15001 AND 20000 
                    THEN CONCAT(cust_2_id, '-LIST-04')
                WHEN b.rn BETWEEN 20001 AND 25000 
                    THEN CONCAT(cust_2_id, '-LIST-05')
                WHEN b.rn BETWEEN 25001 AND 30000 
                    THEN CONCAT(cust_2_id, '-LIST-06')
                WHEN b.rn BETWEEN 30001 AND 35000 
                    THEN CONCAT(cust_2_id, '-LIST-07')
                WHEN b.rn BETWEEN 35001 AND 40000 
                    THEN CONCAT(cust_2_id, '-LIST-08')
                WHEN b.rn BETWEEN 40001 AND 45000 
                    THEN CONCAT(cust_2_id, '-LIST-09')
                WHEN b.rn BETWEEN 45001 AND 50000 
                    THEN CONCAT(cust_2_id, '-LIST-10')
                WHEN b.rn BETWEEN 50001 AND 55000 
                    THEN CONCAT(cust_2_id, '-LIST-11')
                ELSE 'LIST-NEW'
            END AS new_group
        FROM (
            SELECT 
                cust_2_id, 
                cust_3_id, 
                ROW_NUMBER() OVER(PARTITION BY cust_2_id ORDER BY cust_3_name, cust_3_id ASC) AS rn
            FROM {g11_db_name}.cust_hier_dim
            WHERE cust_hier_id = '656'
                AND curr_ind = 'Y'
                AND cust_1_id = '9900000001'
                AND cust_2_id IN ('9900000002', '9900000007')
                AND cust_3_id NOT IN ('9900000007', '2000373917', '2001338173', '2002178438', '2000822739', '2002217420', '0063009359')
            GROUP BY cust_2_id, cust_3_id, cust_3_name
        ) a
    ) b
    ON ch.cust_2_id = b.cust_2_id
    AND ch.cust_3_id = b.cust_3_id;
    """

    # Execute the SQL query
    spark.sql(query)

    # Log the success message
    logger.info(
        "Data has been successfully loaded into {}.{}".format(
            target_db_name, target_table
        )
    )

    return 0

def validate_data(spark, g11_db_name):
    """
    Function to validate data before insertion into the target table.
    Checks for missing values or invalid entries.
    """
    # Perform validation queries (this is just a simple example)
    validation_query = f"""
    SELECT COUNT(*) 
    FROM {g11_db_name}.cust_hier_dim 
    WHERE cust_1_id IS NULL OR cust_2_id IS NULL OR cust_3_id IS NULL
    """
    
    # Execute validation query
    invalid_records = spark.sql(validation_query).collect()[0][0]
    
    # Log the validation result
    if invalid_records > 0:
        logger.warning(f"Data validation failed: {invalid_records} records have missing values.")
        return False
    else:
        logger.info("Data validation passed successfully.")
        return True

def main():
    # Initialize Spark session and logging
    spark = get_spark()
    dbutils = get_dbutils()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Load configuration (as you requested)
    config = Configuration.load_for_default_environment(__file__, dbutils)

    # Modify the database and table names based on the environment or configuration
    g11_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"
    schema = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = f"{config['tables']['cust_hierarchy656_na_lkp']}"

    # Validate data before performing insert
    if validate_data(spark, g11_db_name):
        # Call the task to insert the data
        insertCustHierarchyTask(
            spark=spark,
            logger=logger,
            g11_db_name=g11_db_name,
            target_db_name=schema,
            target_table=target_table,
        )
    else:
        logger.error("Data validation failed. Aborting insert operation.")

if __name__ == "__main__":
    # Main function starts the process
    main()
