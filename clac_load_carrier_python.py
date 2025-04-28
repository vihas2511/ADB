import logging
from typing import List
import pyspark.sql.functions as f
from pyspark.sql import Window
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_tw_fa_carrier_dashboard.common import get_dbutils, get_spark

def get_tac_lane_detail_star(spark, logger, trans_vsblt_db_name, target_db_name):
    logger.info("Reading TAC Tender PG Summary data...")
    
    tac_tender_pg_summary_df = (
        spark.table(f"{target_db_name}.tac_tender_pg_summary_new_star")
        .drop(
            "actual_carr_trans_cost_amt", "linehaul_cost_amt", "incrmtl_freight_auction_cost_amt",
            "cnc_carr_mix_cost_amt", "unsource_cost_amt", "fuel_cost_amt", "acsrl_cost_amt",
            "forward_agent_id", "service_tms_code", "sold_to_party_id", "ship_cond_val",
            "primary_carr_flag", "month_type_val", "cal_year_num", "month_date", "week_num",
            "dest_postal_code"
        )
        .withColumnRenamed("region_code", "state_province_code")
        .distinct()
    )

    logger.info("Adding calculated columns to TAC Tender PG Summary...")
    tac_tender_pg_summary_calc_df = (
        tac_tender_pg_summary_df
        .withColumn("calendar_week_num", f.weekofyear("week_begin_date"))
        .withColumn("calendar_year_num", f.year("week_begin_date"))
        .withColumn("str_calendar_week_num", f.lpad("calendar_week_num", 2, '0'))
        .withColumn("concat_week_year", f.concat(f.col("calendar_year_num"), f.col("str_calendar_week_num")))
        .withColumn("drank_week_year", f.dense_rank().over(Window.orderBy(f.col("concat_week_year").desc())))
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.current_timestamp(), "PRT"))
    )

    logger.info("TAC Lane Detail transformation completed.")
    return tac_tender_pg_summary_calc_df.filter(f.col("drank_week_year") < 14)

def get_tac_shpmt_detail_star(spark, logger, trans_vsblt_db_name, target_db_name):
    logger.info("Reading TAC data...")

    tac_df = (
        spark.table(f"{target_db_name}.tac")
        .withColumn("load_id", f.regexp_replace("load_id", '^0', ''))
        .drop(
            "actual_carr_trans_cost_amt", "linehaul_cost_amt", "incrmtl_freight_auction_cost_amt",
            "cnc_carr_mix_cost_amt", "unsource_cost_amt", "fuel_cost_amt", "acsrl_cost_amt",
            "applnc_subsector_step_cnt", "baby_care_subsector_step_cnt", "chemical_subsector_step_cnt",
            "fabric_subsector_step_cnt", "family_subsector_step_cnt", "fem_subsector_step_cnt",
            "hair_subsector_step_cnt", "home_subsector_step_cnt", "oral_subsector_step_cnt",
            "phc_subsector_step_cnt", "shave_subsector_step_cnt", "skin_subsector_cnt",
            "other_subsector_cnt", "customer_lvl1_code", "customer_lvl1_desc", "customer_lvl2_code",
            "customer_lvl2_desc", "customer_lvl3_code", "customer_lvl3_desc", "customer_lvl4_code",
            "customer_lvl4_desc", "customer_lvl5_code", "customer_lvl5_desc", "customer_lvl6_code",
            "customer_lvl6_desc", "customer_lvl7_code", "customer_lvl7_desc", "customer_lvl8_code",
            "customer_lvl8_desc", "customer_lvl9_code", "customer_lvl9_desc", "customer_lvl10_code",
            "customer_lvl10_desc", "customer_lvl11_code", "customer_lvl11_desc", "customer_lvl12_code",
            "customer_lvl12_desc", "origin_zone_code", "daily_award_qty"
        )
        .distinct()
    )

    logger.info("Adding calculated columns to TAC data...")
    cd_shpmt_tac_calc_df = (
        tac_df
        .withColumn("calendar_week_num", f.weekofyear("actual_goods_issue_date"))
        .withColumn("calendar_year_num", f.year("actual_goods_issue_date"))
        .withColumn("str_calendar_week_num", f.lpad("calendar_week_num", 2, '0'))
        .withColumn("concat_week_year", f.concat(f.col("calendar_year_num"), f.col("str_calendar_week_num")))
        .drop("calendar_week_num", "calendar_year_num", "str_calendar_week_num")
    )

    logger.info("Reading Shipping Location NA Dim data...")
    shipping_location_na_dim_df = (
        spark.table(f"{trans_vsblt_db_name}.shipping_location_na_dim")
        .drop("origin_zone_ship_from_code", "loc_id", "loc_name")
        .withColumnRenamed("postal_code", "final_stop_postal_code")
        .distinct()
    )

    logger.info("Joining TAC data with Lane Detail and Shipping Location...")
    tac_lane_detail_star_df = get_tac_lane_detail_star(spark, logger, trans_vsblt_db_name, target_db_name)

    cd_shpmt_join_df = (
        cd_shpmt_tac_calc_df
        .join(tac_lane_detail_star_df, "concat_week_year", "inner")
        .join(shipping_location_na_dim_df, "final_stop_postal_code", "left")
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.current_timestamp(), "PRT"))
    )

    return cd_shpmt_join_df

def load_carrier_dashboard():
    spark = get_spark()
    dbutils = get_dbutils()

    config = Configuration.load_for_default_environment(__file__, dbutils)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    trans_vsblt_db_name = f"{config['src-catalog-name']}.{config['trans_vsblt_db_name']}"
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"

    logger.info(f"Source DB: {trans_vsblt_db_name}")
    logger.info(f"Target DB: {target_db_name}")

    logger.info(f"Starting load for tac_lane_detail_star...")

    tac_lane_detail_star_df = get_tac_lane_detail_star(
        spark=spark,
        logger=logger,
        trans_vsblt_db_name=trans_vsblt_db_name,
        target_db_name=target_db_name,
    )

    tac_lane_detail_star_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{target_db_name}.tac_lane_detail_star"
    )

    logger.info(f"Loading tac_lane_detail_star table completed.")

    logger.info(f"Starting load for tac_shpmt_detail_star...")

    tac_shpmt_detail_star_df = get_tac_shpmt_detail_star(
        spark=spark,
        logger=logger,
        trans_vsblt_db_name=trans_vsblt_db_name,
        target_db_name=target_db_name,
    )

    tac_shpmt_detail_star_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{target_db_name}.tac_shpmt_detail_star"
    )

    logger.info(f"Loading tac_shpmt_detail_star table completed.")

if __name__ == "__main__":
    load_carrier_dashboard()
