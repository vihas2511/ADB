import logging
from typing import Dict, List, Optional, Union

import pyspark.sql.functions as f
from pyspark.sql import Window
from pg_composite_pipelines_configuration.configuration import Configuration

from pg_tw_fa_marm_reporting.common import get_dbutils, get_spark
import expr_on_time_data_hub as expr
import get_src_data.get_rds as rds
import get_src_data.get_transfix as tvb


def load_on_time_data_hub_star(spark, logger, config, debug_mode_ind, debug_postfix):
    rds_db_name = config["rds-db-name"]
    trans_vsblt_db_name = config["trans-vsblt-db-name"]
    target_db_name = config["target-db-name"]
    staging_location = config["staging-location"]

    logger.info("Reading customer dimension...")
    cust_dim_df = rds.get_cust_dim(logger, spark, rds_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)

    trade_chanl_hier_dim_df = rds.get_trade_chanl_hier_dim(logger, spark, rds_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)

    logger.info("Reading shipping location data...")
    ship_loc_df = tvb.get_shipping_location_na_dim(logger, spark, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)
    ship_loc_df = ship_loc_df.drop("loc_name", "state_province_code", "postal_code")\
                             .withColumnRenamed("loc_id", "ship_point_code")\
                             .withColumnRenamed("origin_zone_ship_from_code", "sl_origin_zone_ship_from_code")

    logger.info("Reading other source data...")
    sambc_df = tvb.get_sambc_master(logger, spark, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)
    sambc_df = sambc_df.select("customer_lvl3_desc", "sambc_flag")

    csot_bucket_final_df = tvb.get_csot_bucket(logger, spark, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)
    csot_bucket_final_df = csot_bucket_final_df\
        .withColumn("poloadid_join", f.col("poload_id"))\
        .withColumn("cust_po_num", f.col("poload_id"))\
        .withColumn("load_id", f.col("poload_id"))\
        .withColumnRenamed("cust_po_num", "pg_order_num")\
        .withColumnRenamed("poload_id", "poload_id_new")

    actual_ship_time_df = tvb.get_otd_vfr_na_star(logger, spark, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)
    actual_ship_time_df = actual_ship_time_df\
        .select("shpmt_id", "actual_ship_datetm").distinct()\
        .withColumn("load_id", f.expr(expr.load_id_expr))\
        .withColumn("actual_ship_datetm", f.expr(expr.actual_ship_datetm_expr)).drop("shpmt_id")

    on_time_df = tvb.get_on_time_arriv_shpmt_custshpmt_na_star(logger, spark, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)
    on_time_df = on_time_df\
        .withColumn("load_id", f.substring(f.col('shpmt_id'), -9, 9))\
        .withColumn("str_carr_num", f.col('carr_num').cast("int"))\
        .withColumn("poloadid_join", f.concat(f.col('pg_order_num'), f.col('load_id')))\
        .withColumn("poload_id", f.concat(f.col('pg_order_num'), f.col('load_id')))\
        .withColumn("order_create_tmstp", f.concat(f.col('order_create_date'), f.lit(' '), f.col('order_create_datetm')))\
        .withColumn("schedule_tmstp", f.concat(f.col('plan_shpmt_start_date'), f.lit(' '), f.col('plan_shpmt_start_datetm')))\
        .withColumn("actual_dlvry_tmstp", f.concat(f.col('actual_shpmt_end_date'), f.lit(' '), f.col('actual_shpmt_end_aot_datetm')))\
        .withColumn("shpmt_cnt", f.expr(expr.no_shpmt_expr))\
        .withColumnRenamed("ship_to_party_id", "customer_id")

    on_time_final_df = on_time_df.groupBy("load_id", "trnsp_stage_num")\
        .agg(f.max("event_datetm").alias("last_appt_dlvry_tmstp"), f.min("event_datetm").alias("first_appt_dlvry_tmstp"))

    tfs_df = tvb.get_tfs(logger, spark, target_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)\
        .select("shpmt_id", "freight_auction_val").filter('freight_auction_val = "YES"')\
        .withColumn("load_id", f.regexp_replace("shpmt_id", '^0', '')).drop("shpmt_id").distinct()

    tac_df = tvb.get_tac(logger, spark, target_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix)

    tac_calcs_df = tac_df.groupBy("load_id").agg(
        f.max("actual_carr_trans_cost_amt").alias("actual_carr_trans_cost_amt"),
        f.max("linehaul_cost_amt").alias("linehaul_cost_amt")
    )

    tac_avg_df = tac_df.selectExpr(
        "load_id", "forward_agent_id AS str_carr_num", "service_tms_code AS actual_service_tms_code", "avg_award_weekly_vol_qty")\
        .distinct()\
        .withColumn("primary_carr_flag", f.expr(expr.carr_flag_expr))

    final_df = on_time_df\
        .join(tfs_df, "load_id", "left")\
        .join(tac_calcs_df, "load_id", "left")\
        .join(tac_avg_df, ["load_id", "str_carr_num"], "left")\
        .join(actual_ship_time_df, "load_id", "left")\
        .join(on_time_final_df, ["load_id", "trnsp_stage_num"], "left")\
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.current_timestamp(), spark.conf.get("spark.sql.session.timeZone")))

    logger.info("Loading final DataFrame to table...")
    target_table_name = f"{target_db_name}.on_time_data_hub_star"
    target_columns = spark.read.table(target_table_name).columns

    final_df = final_df.select(target_columns)
    final_df.write.format("delta").insertInto(tableName=target_table_name, overwrite=True)

    logger.info("Successfully loaded data into %s", target_table_name)
    return 0


def main():
    spark = get_spark()
    dbutils = get_dbutils()
    config = Configuration.load_for_default_environment(__file__, dbutils)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    debug_mode_ind = 0
    debug_postfix = ""

    load_on_time_data_hub_star(
        spark=spark,
        logger=logger,
        config=config,
        debug_mode_ind=debug_mode_ind,
        debug_postfix=debug_postfix,
    )


if __name__ == "__main__":
    main()
