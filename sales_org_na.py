import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType
#changes artemis file
from pg_tw_fa_artemis.common import get_dbutils, get_spark

#Change the source_db_name into like rds 
def insertSalesOrgNaDimTask(spark, logger, source_db_name, target_db_name, target_table):
    query = f"""
    SELECT
        tvko.sales_org_code AS sales_organization,
        tvko.company_code AS company_code_of_the_sales_organization,
        t001.country_code AS country_key,
        t001.currency_code AS currency_key,
        t001.fiscal_year_variant_code AS fiscal_year_variant,
        tvko.customer_id AS customer_number_for_intercompany_billing,
        tvko.currency_code AS statistics_currency
    FROM {source_db_name}.sales_org_dim AS tvko
    LEFT JOIN {source_db_name}.company_code_dim AS t001
        ON tvko.company_code = t001.company_code
    """

    sales_org_na_dim_df = spark.sql(query)

    sales_org_na_dim_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{target_db_name}.{target_table}"
    )

    logger.info(
        "Data has been successfully loaded into {}.{}".format(
            target_db_name, target_table
        )
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

    config = Configuration.load_for_default_environment(__file__, dbutils)

    source_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"  # or change to actual source like dbOsiNa
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['sales_org_na_dim']  # Ensure this key exists

    insertSalesOrgNaDimTask(
        spark=spark,
        logger=logger,
        source_db_name=source_db_name,
        target_db_name=target_db_name,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
