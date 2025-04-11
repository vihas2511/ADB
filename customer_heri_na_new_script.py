import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType

from pg_tw_fa_artemis.common import get_dbutils, get_spark


def insertCustHierarchy656NaLkpTask(spark, logger, db_rds, target_db_name, target_table):

    query = f"""
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
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 3 THEN sub.new_group
            WHEN ch.cust_orig_lvl >= 3 THEN ch.cust_3_id
            ELSE ''
        END AS cust_3_id,
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 3 THEN sub.new_group
            WHEN ch.cust_orig_lvl >= 3 THEN ch.cust_3_name
            ELSE ''
        END AS cust_3_name,
        -- (Repeat cust_4 to cust_12 fields here...)
        CASE 
            WHEN sub.new_group IS NOT NULL AND ch.cust_orig_lvl >= 12 THEN ch.cust_11_name
            WHEN ch.cust_orig_lvl >= 12 THEN ch.cust_12_name
            ELSE ''
        END AS cust_12_name
    FROM (
        SELECT 
            cust_1_id, cust_1_name,
            cust_2_id, cust_2_name,
            cust_3_id, cust_3_name,
            cust_4_id, cust_4_name,
            cust_5_id, cust_5_name,
            cust_6_id, cust_6_name,
            cust_7_id, cust_7_name,
            cust_8_id, cust_8_name,
            cust_9_id, cust_9_name,
            cust_10_id, cust_10_name,
            cust_11_id, cust_11_name,
            cust_12_id, cust_12_name,
            CASE 
                WHEN cust_2_id IN ('9900000002', '9900000007') 
                    THEN cust_orig_lvl + 1
                ELSE cust_orig_lvl
            END AS cust_orig_lvl
        FROM {db_rds}.cust_hier_dim
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
                WHEN b.rn < 5001 THEN CONCAT(cust_2_id, '-LIST-01')
                WHEN b.rn BETWEEN 5001 AND 10000 THEN CONCAT(cust_2_id, '-LIST-02')
                WHEN b.rn BETWEEN 10001 AND 15000 THEN CONCAT(cust_2_id, '-LIST-03')
                WHEN b.rn BETWEEN 15001 AND 20000 THEN CONCAT(cust_2_id, '-LIST-04')
                WHEN b.rn BETWEEN 20001 AND 25000 THEN CONCAT(cust_2_id, '-LIST-05')
                WHEN b.rn BETWEEN 25001 AND 30000 THEN CONCAT(cust_2_id, '-LIST-06')
                WHEN b.rn BETWEEN 30001 AND 35000 THEN CONCAT(cust_2_id, '-LIST-07')
                WHEN b.rn BETWEEN 35001 AND 40000 THEN CONCAT(cust_2_id, '-LIST-08')
                WHEN b.rn BETWEEN 40001 AND 45000 THEN CONCAT(cust_2_id, '-LIST-09')
                WHEN b.rn BETWEEN 45001 AND 50000 THEN CONCAT(cust_2_id, '-LIST-10')
                WHEN b.rn BETWEEN 50001 AND 55000 THEN CONCAT(cust_2_id, '-LIST-11')
                ELSE 'LIST-NEW'
            END AS new_group
        FROM (
            SELECT 
                cust_2_id, 
                cust_3_id, 
                ROW_NUMBER() OVER (PARTITION BY cust_2_id ORDER BY cust_3_name, cust_3_id ASC) AS rn
            FROM (
                SELECT 
                    cust_2_id, 
                    cust_3_id, 
                    cust_3_name
                FROM {db_rds}.cust_hier_dim
                WHERE cust_hier_id = '656'
                    AND curr_ind = 'Y'
                    AND cust_1_id = '9900000001'
                    AND cust_2_id IN ('9900000002', '9900000007')
                    AND cust_3_id NOT IN ('9900000007', '2000373917', '2001338173', '2002178438', '2000822739', '2002217420', '0063009359')
                GROUP BY cust_2_id, cust_3_id, cust_3_name
            ) a
        ) b
    ) sub
    ON ch.cust_2_id = sub.cust_2_id AND ch.cust_3_id = sub.cust_3_id
    """

    cust_hierarchy_df = spark.sql(query)

    cust_hierarchy_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{target_db_name}.{target_table}"
    )

    logger.info(f"Data successfully written to {target_db_name}.{target_table}")
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

    config = Configuration.load_for_default_environment(__file__, dbutils)

    db_rds = f"{config['src-catalog-name']}.{config['rds-db-name']}"
    schema = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['cust_hierarchy656_na_lkp']

    insertCustHierarchy656NaLkpTask(
        spark=spark,
        logger=logger,
        db_rds=db_rds,
        target_db_name=schema,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
