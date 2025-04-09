import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType

from pg_tw_fa_artemis.common import get_dbutils, get_spark


def insertProdAltUomSdimTask(spark, logger, g11_db_name, target_db_name, target_table):

    query = f"""
    SELECT 
        marm_out.prod_id, 
        marm_out.alt_uom,
        IF(marm_out.denominator_alt_uom_buom_val > 0, 
            CAST(marm_out.numerator_alt_uom_buom_val AS DECIMAL(18,12)) / marm_out.denominator_alt_uom_buom_val, 
            NULL) AS alt_uom_buom_factor,
        IF(marm_out.numerator_alt_uom_buom_val > 0, 
            CAST(marm_out.denominator_alt_uom_buom_val AS DECIMAL(18,12)) / marm_out.numerator_alt_uom_buom_val, 
            NULL) AS buom_alt_uom_factor,
        marm_out.numerator_alt_uom_buom_val,
        marm_out.denominator_alt_uom_buom_val
    FROM (
        SELECT 
            marm.matnr AS prod_id, 
            marm.meinh AS alt_uom,
            marm.umren AS denominator_alt_uom_buom_val,
            marm.umrez AS numerator_alt_uom_buom_val
        FROM 
            {g11_db_name}.marm marm
        JOIN (
            SELECT 
                mara.matnr AS prod_id
            FROM 
                {g11_db_name}.mara mara
            WHERE 
                mara.mtart = 'FERT' 
                AND mara.simp_chng_type_code NOT IN ('I', 'U', 'D')
        UNION
        select prod_id
				from(SELECT mara.matnr as prod_id, ROW_NUMBER() over(partition by mara.matnr order by mara.bd_mod_utc_time_stamp desc )as rnk
             				FROM {g11_db_name}.mara mara
				where  mara.simp_chng_type_code in ('I','U') and mara.mtart = 'FERT' )
                where rnk=1 		
              ) ap
           ON marm.matnr = ap.prod_id
         WHERE marm.simp_chng_type_code != 'D'
      ) marm_out  
    """

    prod_alt_uom_sdim = spark.sql(query)

    prod_alt_uom_sdim.write.format("delta").mode("overwrite").saveAsTable(
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

    g11_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"

    schema = f"{config['catalog-name']}.{config['schema-name']}"

    target_table = f"{config['tables']['prod_alt_uom_sdim']}"

    insertProdAltUomSdimTask(
        spark=spark,
        logger=logger,
        g11_db_name=g11_db_name,
        target_db_name=schema,
        target_table=target_table,
    )


if __name__ == "__main__":
    # if you need to read params from your task/workflow, use sys.argv[] to retrieve them and pass them to main here
    # eg sys.argv[0] for first positional param
    main()