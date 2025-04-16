import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType

from pg_tw_fa_artemis.common import get_dbutils, get_spark

def insertOperationalTariffFilterStarTask(spark, logger, source_db_name, target_db_name, target_table):
    query = f"""
    WITH raw_source AS (
        SELECT
            CASE
                WHEN source.report_date >= '2018-06-01' AND source.report_date < '2018-09-10'
                THEN source.origin_corp_code
                ELSE source.lane_origin_zone_code
            END AS `lane origin zone`,
            source.lane_dstn_zone_id AS `lane destination zone`,
            source.origin_corp_code AS `origin corporate id 2`,
            source.dest_zip_code AS `destination zip`,
            source.dest_loc_desc AS `dest location description`,
            source.dest_corp_code AS `dest corporate id2`,
            source.exclude_from_optimization_flag AS `exclude from optimization`,
            source.exclude_from_cst_flag AS `exclude from cst`,
            source.tariff_id AS `tariff number`,
            source.carrier_id AS `carrier id`,
            source.carrier_name AS `carrier description`,
            source.service_code AS service,
            source.service_desc AS `service description`,
            source.cea_profile_code AS `cea profile id`,
            source.train_operating_day_qty AS `number of train op days`,
            source.train_day_of_week_list AS `train days of week`,
            source.transit_time_val AS `transit time`,
            source.charge_code, source.charge_desc,
            source.mile_contract_rate AS rate,
            source.payment_crncy_code AS `payment currency`,
            source.rate_unit_code AS `rating unit`,
            source.base_charge_amt AS `base charge`,
            source.min_charge_amt AS `minimum charge`,
            source.rate_eff_date AS `rate effective date`,
            source.rate_exp_date AS `rate exp date`,
            source.min_weight_qty AS `minimum weight`,
            source.min_floor_postn_cnt AS `minimum floor positions`,
            source.max_floor_postn_cnt AS `maximum floor positions`,
            source.max_weight_qty AS `maximum weight`,
            source.auto_accept_tender_override_flag AS `auto-accept tender override`,
            source.freight_auction_eligibility_code AS `freight auction eligibility`,
            source.alloc_type_code AS `allocation type`,
            source.alloc_profile_val AS `allocation profile basis`,
            source.award_rate AS awards,
            source.weekly_alloc_pct AS `percent weekly allocation`,
            source.mon_min_load_qty AS `mon min (loads)`,
            source.mon_max_load_qty AS `mon max (loads)`,
            source.tue_min_load_qty AS `tue min (loads)`,
            source.tue_max_load_qty AS `tue max (loads)`,
            source.wed_min_load_qty AS `wed min (loads)`,
            source.wed_max_load_qty AS `wed max (loads)`,
            source.thu_min_load_qty AS `thu min (loads)`,
            source.thu_max_load_qty AS `thu max (loads)`,
            source.fri_min_load_qty AS `fri min (loads)`,
            source.fri_max_load_qty AS `fri max (loads)`,
            source.sat_min_load_qty AS `sat min (loads)`,
            source.sat_max_load_qty AS `sat max (loads)`,
            source.sun_min_load_qty AS `sun min (loads)`,
            source.sun_max_load_qty AS `sun max (loads)`,
            source.dlvry_schedule_code AS `delivery schedule id`,
            source.equip_type_code AS `equipment type`,
            source.status_code AS status,
            source.rate_code,
            source.service_grade_val AS `service grade`,
            source.tariff_desc AS `tariff id`,
            source.max_no_of_shpmt_cnt AS `maximum no of shipments`,
            source.cust_id AS `customer id`,
            source.cust_desc AS `customer description`,
            source.equip_code AS `equipment code`,
            source.resource_project_code AS `resource project id`,
            source.report_date
        FROM {source_db_name}.operational_tariff_filter_star AS source
    ),
    adjusted_source AS (
        SELECT
            `lane origin zone`,
            `lane destination zone`,
            CASE WHEN (`origin corporate id 2` IS NULL OR `origin corporate id 2` = '') THEN `lane origin zone` ELSE `origin corporate id 2` END AS `origin corporate id 2`,
            `destination zip`,
            `dest location description`,
            CASE WHEN (`dest corporate id2` NOT LIKE 'SF_%' OR `dest corporate id2` IS NULL OR `dest corporate id2` = '') THEN `lane destination zone` ELSE `dest corporate id2` END AS `dest corporate id2`,
            *
        FROM raw_source
    ),
    operational_tariff_filter_withSFLANE AS (
        SELECT
            CONCAT(LEFT(adjusted_source.`origin corporate id 2`, 20), '-', LEFT(adjusted_source.`dest corporate id2`, 20)) AS `SF Lane`,
            adjusted_source.*
        FROM adjusted_source
    ),
    operational_tariff_Zone_Awards AS (
        SELECT
            `SF Lane`, `report date`, `origin corporate id 2`, `dest corporate id2`,
            MAX(awards) AS Awards
        FROM operational_tariff_filter_withSFLANE
        GROUP BY `SF Lane`, `report date`, `lane origin zone`, `lane destination zone`, `origin corporate id 2`, `dest corporate id2`, `carrier id`, `service`
    ),
    operational_tariff_Total_SF_Awards_Volume AS (
        SELECT
            `SF Lane`, `report date`, SUM(Awards) AS `TOTAL SF Award Volume`
        FROM operational_tariff_Zone_Awards
        GROUP BY `SF Lane`, `report date`, `origin corporate id 2`, `dest corporate id2`
    )
    SELECT OT.*, SFA.`TOTAL SF Award Volume` AS TOTAL_SF_Award_Volume
    FROM operational_tariff_filter_withSFLANE OT
    LEFT JOIN operational_tariff_Total_SF_Awards_Volume SFA
      ON OT.`SF Lane` = SFA.`SF Lane` AND OT.`report date` = SFA.`report date`
    """

    result_df = spark.sql(query)

    result_df.write.format("delta").mode("overwrite").saveAsTable(f"{target_db_name}.{target_table}")

    logger.info("Data has been successfully loaded into {}.{}".format(target_db_name, target_table))


def main():
    spark = get_spark()
    dbutils = get_dbutils()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    config = Configuration.load_for_default_environment(__file__, dbutils)

    source_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['operational_tariff_filter_star']

    insertOperationalTariffFilterStarTask(spark, logger, source_db_name, target_db_name, target_table)


if __name__ == "__main__":
    main()
