import logging
from typing import Dict, List, Optional, Union

import pyspark.sql.functions as f
from pg_composite_pipelines_configuration.configuration import Configuration

from pg_tw_fa_marm_reporting.common import get_dbutils, get_spark


def load_tranfix_metrics(spark, logger, source_table: str, target_db_name: str, target_table: str):
    logger.info("Reading base Tranfix data from table: %s", source_table)
    
    df = spark.read.table(source_table)

    # Applying expressions
    df_transformed = (
        df
        .withColumn("load_id", f.expr("SUBSTR(shpmt_id,-9)"))
        .withColumn("carrier_description", f.expr("""CASE 
            WHEN carr_num = 15329857 THEN 'CIRCLE 8 LOGISTICS INC'
            WHEN carr_num = 15322444 THEN 'GREYPOINT INC'
            WHEN carr_num = 15327462 THEN 'DUPRE LOGISTICS, LLC'
            WHEN carr_num = 15169031 THEN 'TOTAL QUALITY LOGISTICS LLC'
            WHEN carr_num = 15306674 THEN 'UNIVERSAL TRUCKLOAD INC'
            WHEN carr_num = 15164435 THEN 'US XPRESS INC'
            WHEN carr_num = 20512523 THEN 'US XPRESS INC'
            ELSE carr_desc END"""))
        .withColumn("true_frt_type_desc", f.expr("""CASE 
            WHEN CAST(COALESCE(REGEXP_EXTRACT(ship_to_party_code, '^[0-9]', 0), '-1') AS INT) >= 0 THEN 'CUSTOMER'
            ELSE 'INTERPLANT' END"""))
        .withColumn("customer_description", f.expr("""CASE 
            WHEN customer_code LIKE 'CARDINAL%' THEN customer_lvl3_desc
            WHEN customer_code LIKE 'WALMART%' THEN 'WALMART'
            WHEN customer_code LIKE 'COSTCO COMPANIES%%' THEN 'COSTCO'
            WHEN customer_code LIKE 'JETRO%' THEN 'JETRO'
            WHEN customer_code LIKE 'TIENDAS SINDICALES%' THEN 'TIENDAS SINDICALES'
            ELSE customer_code END"""))
        .withColumn("origin_zone_ship_from_code", f.expr("""CASE
            WHEN substr(coalesce(origin_zone_code, ''), 1, 3) <> 'SF_' THEN sl_origin_zone_ship_from_code
            ELSE origin_zone_name END"""))
        .withColumn("actual_ship_datetm", f.expr("""CASE 
            WHEN SUBSTR(actual_ship_datetm, 3, 1) = ':' THEN actual_ship_datetm 
            ELSE CONCAT(SUBSTR(actual_ship_datetm,1,2), ':', SUBSTR(actual_ship_datetm,3,2), ':', SUBSTR(actual_ship_datetm,5,2)) END"""))
        .withColumn("true_fa_flag", f.expr("""CASE
            WHEN freight_auction_val = 'YES' THEN 'Y'
            ELSE 'N' END"""))
        .withColumn("carrier_flag", f.expr("""CASE
            WHEN avg_award_weekly_vol_qty > 0.01 THEN 1
            ELSE 0 END"""))
        .withColumn("min_rn_code", f.expr("""CASE
            WHEN min_event_datetm_rn = 1 THEN plan_shpmt_end_tmstp_calc
            ELSE NULL END"""))
        .withColumn("max_rn_code", f.expr("""CASE
            WHEN max_event_datetm_rn = 1 THEN plan_shpmt_end_tmstp_calc
            ELSE NULL END"""))
        .withColumn("event_count", f.expr("""CASE
            WHEN ship_cond_code = '' THEN 0
            ELSE otd_cnt END + CASE
            WHEN lot_ontime_status_last_appt_val = '' THEN 0
            ELSE tat_late_counter_val END"""))
        .withColumn("tms_code", f.expr("""CASE
            WHEN max_event_datetm_rn = 1 THEN service_tms_code
            ELSE NULL END"""))
        .withColumn("fa_flag", f.expr("""CASE
            WHEN max_frt_auction_code = 'NO' THEN max_frt_auction_code
            WHEN max_frt_auction_code = 'YES' THEN max_frt_auction_code
            ELSE NULL END"""))
        .withColumn("pct_lot", f.expr("max_otd_cnt / max_shpmt_cnt * 100"))
        .withColumn("ci_code", f.expr("""CASE
            WHEN frt_type_code = 'C' THEN 'CUSTOMER'
            WHEN frt_type_code = 'I' THEN 'INTERPLANT'
            WHEN frt_type_code = 'E' THEN 'EXPORT'
            ELSE '' END"""))
        .withColumn("bin_code", f.expr("""CASE
            WHEN distance_qty < 250 THEN '< 250'
            WHEN distance_qty < 500 THEN '250 - 499'
            WHEN distance_qty < 1000 THEN '500 - 1000'
            ELSE '> 1000' END"""))
        .withColumn("flag_case_code", f.expr("""CASE
            WHEN child_shpmt_num = '' THEN ''
            ELSE shpmt_id END"""))
        .withColumn("no_shpmt", f.expr("""CASE
            WHEN ship_cond_code = '' THEN 0
            ELSE otd_cnt END + CASE
            WHEN lot_ontime_status_last_appt_val = '' THEN 0
            ELSE tat_late_counter_val END"""))
        .withColumn("aot_meas_flag", f.expr("""CASE
            WHEN first_appt_dlvry_tmstp = '' AND actual_dlvry_tmstp = '' THEN 0
            WHEN request_dlvry_to_tmstp = '' AND actual_dlvry_tmstp = '' THEN 0
            ELSE 1 END"""))
        .withColumn("iot_meas_flag", f.expr("""CASE
            WHEN frt_type_desc IN ('INTERPLANT', 'EXPORT') AND first_appt_dlvry_tmstp = '' AND actual_dlvry_tmstp = '' THEN 0
            WHEN frt_type_desc IN ('INTERPLANT', 'EXPORT') AND request_dlvry_to_tmstp = '' AND actual_dlvry_tmstp = '' THEN 0
            WHEN frt_type_desc IN ('INTERPLANT', 'EXPORT') THEN 1
            ELSE 0 END"""))
        .withColumn("lot_meas_flag", f.expr("""CASE
            WHEN final_lrdt_tmstp = '' AND actual_load_end_tmstp = '' THEN 0
            ELSE 1 END"""))
        .withColumn("dest_ship_from_code", f.expr("""CASE
            WHEN true_frt_type_desc = 'CUSTOMER' THEN ship_to_party_code
            WHEN true_frt_type_desc = 'INTERPLANT' THEN customer_desc
            ELSE NULL END"""))
    )

    df_transformed = df_transformed.distinct()

    logger.info("Writing transformed data to Delta table %s.%s", target_db_name, target_table)
    df_transformed.write.format("delta").mode("overwrite").insertInto(f"{target_db_name}.{target_table}")

    logger.info("Tranfix metrics successfully written.")
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

    source_table = f"{config['src-catalog-name']}.{config['tranfix_base_table']}"
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['tranfix_metrics_star']

    logger.info("Source table: %s", source_table)
    logger.info("Target DB: %s, Table: %s", target_db_name, target_table)

    load_tranfix_metrics(
        spark=spark,
        logger=logger,
        source_table=source_table,
        target_db_name=target_db_name,
        target_table=target_table
    )

if __name__ == "__main__":
    main()
