import logging
import sys
# Need to change config file
from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import SparkSession
from pg_tw_fa_artemis.common import get_dbutils, get_spark

def insertPlantNaDimTask(spark, logger, g11_db_name, target_db_name, target_table):
    query = f"""
    INSERT OVERWRITE TABLE {target_db_name}.plant_na_dim
    SELECT
        t001w.plant_id AS plant,
        t001w.city_name AS city,
        t001k.company_code,
        t001w.country_code AS country_key,
        t001w.county_code AS county_code,
        t001w.distrib_channel_code AS distribution_channel_for_intercompany_billing,
        t001w.division_code AS division_for_intercompany_billing,
        t001w.factory_cal_key_code AS factory_calendar_key,
        t001w.site_code AS valuation_area,
        t001w.lang_code AS language_key,
        t001w.city_code,
        t001w.part1_name AS name,
        t001w.part2_name AS name_2,
        t001w.maint_plan_plant_id AS maintenance_planning_plant,
        t001w.plant_categ_code AS plant_category,
        t001w.post_office_box_code AS po_box,
        t001w.postal_code,
        t001w.purchase_org_code AS purchasing_organization,
        t001w.region_name AS region_state_province_county,
        t001w.customer_id AS customer_number_of_plant,
        t001w.sales_org_code AS sales_organization_for_intercompany_billing,
        t001w.sales_district_code AS sales_district,
        t001w.street_name AS house_number_and_street,
        t001w.tax_jrsdct_code AS tax_jurisdiction,
        t001w.vendor_id AS vendor_number_of_plant,
        t001w.variance_key_code AS variance_key,
        t001w.activtng_reqmt_plan_flag AS activating_requirements_planning,
        regexp_replace(t001w.po_tolrnc_day_cnt,'^0+$','') AS number_of_days_for_po_tolerance_compress_info_records_su,
        t001w.take_regular_vendor_into_account_flag AS take_regular_vendor_into_account,
        t001w.batch_status_mgmt_active_flag AS indicator_batch_status_management_active_1,
        t001w.cond_plant_lvl_code AS indicator_conditions_at_plant_level,
        t001w.source_list_reqmt_flag AS indicator_source_list_requirement,
        cast(regexp_replace(t001w.first_reminder_expediter_day_cnt,'^0+$','') as decimal(3,0)) AS number_of_days_for_first_reminder_expediter,
        cast(regexp_replace(t001w.second_reminder_expediter_day_cnt,'^0+$','') as decimal(3,0)) AS number_of_days_for_second_reminder_expediter,
        cast(regexp_replace(t001w.third_reminder_expediter_day_cnt,'^0+$','') as decimal(3,0)) AS number_of_days_for_third_reminder_expediter,
        t001w.plant_tax_code AS tax_indicator_plant_purchasing,
        t001w.vendor_first_dunning_declare_name AS text_name_of_1st_dunning_of_vendor_declarations,
        t001w.vendor_second_dunning_declare_name AS text_name_of_the_2nd_dunning_of_vendor_declarations,
        t001w.vendor_third_dunning_declare_name AS text_name_of_3rd_dunning_of_vendor_declarations,
        t001w.supply_region_code AS supply_region_region_supplied
    FROM {g11_db_name}.plant_dim AS t001w
    LEFT JOIN {g11_db_name}.vltn_area_dim AS t001k
        ON t001w.sap_client_id  = t001k.sap_client_id
            AND t001w.site_code = t001k.site_code
    """

    # Execute the SQL query
    spark.sql(query)

    logger.info(
        "Data has been successfully loaded into {}.{}".format(
            target_db_name, target_table
        )
    )

    return 0


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

    # Configuration
    config = Configuration.load_for_default_environment(__file__, dbutils)

    g11_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"
    schema = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = f"{config['tables']['plant_na_dim']}"

    insertPlantNaDimTask(
        spark=spark,
        logger=logger,
        g11_db_name=g11_db_name,
        target_db_name=schema,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
