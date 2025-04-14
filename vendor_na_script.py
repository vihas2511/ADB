import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType
#change the artemis file
from pg_tw_fa_artemis.common import get_dbutils, get_spark

#chnage the source_db_name like rds
def insertVendorNaDimTask(spark, logger, source_db_name, target_db_name, target_table):
    query = f"""
    SELECT
        lfa1.vendor_id AS account_number_of_vendor_or_creditor,
        lfa1.vendor_account_group_code AS vendor_account_group,
        lfa1.city_name AS city,
        lfa1.district_name AS district,
        lfa1.country_code AS country_key,
        lfa1.credit_info_num AS credit_information_number,
        lfa1.fax_num AS fax_number,
        lfa1.one_time_account_flag AS indicator_is_the_account_a_one_time_account,
        lfa1.industry_key_code AS industry_key,
        lfa1.lang_code AS language_key,
        lfa1.part1_name AS name_1_1,
        lfa1.part2_name AS name_2_1,
        lfa1.part3_name AS name_3_1,
        lfa1.company_id AS company_id_of_trading_partner,
        lfa1.tel_num AS first_telephone_number,
        lfa1.plant_id AS plant_own_or_external,
        lfa1.post_office_box_code AS po_box,
        lfa1.postal_code,
        lfa1.po_box_postal_code AS p_o_box_postal_code,
        lfa1.region_name AS region_state_province_county,
        lfa1.sort_field_name AS sort_field,
        lfa1.street_name AS house_number_and_street,
        lfa1.tax1_num AS tax_number_1,
        lfa1.tax2_num AS tax_number_2,
        lfa1.master_record_central_delete_flag AS central_deletion_flag_for_master_record,
        lfa1.last_review_extrn_date AS last_review_external,
        lfa1.tax_jrsdct_code AS tax_jurisdiction,
        lfa1.train_station_code AS train_station,
        lfa1.intl_loc_part1_num AS international_location_number_part_1,
        lfa1.intl_loc_part2_num AS international_location_number_part_2,
        lfa1.intl_loc_num_check_digit_val AS check_digit_for_the_international_location_number,
        lfa1.data_comm_line_num AS data_communication_line_no,
        lfa1.data_medium_exchange_code AS indicator_for_data_medium_exchange,
        lfa1.isr_subscriber_num AS isr_subscriber_number,
        lfa1.master_record_account_num AS account_number_of_the_master_record_with_fiscal_address,
        lfa1.withhold_tax_person_birth_date AS date_of_birth_of_the_person_subject_to_withholding_tax,
        lfa1.withhold_tax_person_birth_place_name AS place_of_birth_of_the_person_subject_to_withholding_tax,
        lfa1.one_time_account_ref_account_group_code AS reference_account_group_for_one_time_account_vendor,
        lfa1.alt_payee_account_num AS account_number_of_the_alternative_payee,
        lfa1.vendor_sub_range_relevant_flag AS indicator_vendor_sub_range_relevant,
        lfa1.matchcd_search_term1_name AS search_term_for_matchcode_search_1,
        lfa1.matchcd_search_term2_name AS search_term_for_matchcode_search_2,
        lfa1.matchcd_search_term3_name AS search_term_for_matchcode_search_3,
        lfa1.factory_cal_key_code AS factory_calendar_key,
        lfa1.withhold_tax_person_sex_code AS key_for_the_sex_of_the_person_subject_to_withholding_tax,
        lfa1.central_impose_purchase_block_flag AS centrally_imposed_purchasing_block,
        lfa1.will_block_function_code AS function_that_will_be_blocked,
        lfa1.central_posting_block_flag AS central_posting_block,
        lfa1.business_partner_equalzn_flag AS indicator_business_partner_subject_to_equalization_tax,
        lfa1.natural_person_flag AS natural_person,
        lfa1.vat_liable_flag AS liable_for_vat,
        lfa1.telebox_num AS telebox_number,
        lfa1.second_tel_num AS second_telephone_number,
        lfa1.teletex_num AS teletex_number,
        lfa1.plant_lvl_relevant_flag AS indicator_plant_level_relevant,
        lfa1.allow_alt_payee_flag AS indicator_alternative_payee_in_document_allowed,
        lfb1.minority_indicators,
        lfa1.title_name AS title_1,
        lfa1.data_medium_exchange_instr_key_code AS instruction_key_for_data_medium_exchange,
        lfa1.create_date AS date_on_which_the_record_was_created,
        lfa1.create_by_user_name AS name_of_person_who_created_the_object,
        lfa1.group_key_code AS group_key,
        lfa1.customer_id AS customer_number,
        lfa1.part4_name AS name_4,
        lfa1.telex_num AS telex_number,
        lfa1.vat_registn_num AS vat_registration_number,
        lfa1.po_box_city_name AS po_box_city,
        lfa1.payment_block_flag AS payment_block,
        lfa1.std_carr_access_code AS standard_carrier_access_code
    FROM {source_db_name}.vendor_dim AS lfa1
    LEFT JOIN (
        SELECT 
            vendor_id AS account_number_of_vendor_or_creditor,
            minority_code AS minority_indicators
        FROM {source_db_name}.vendor_company_code_dim
        GROUP BY vendor_id, minority_code
    ) AS lfb1
        ON lfa1.vendor_id = lfb1.account_number_of_vendor_or_creditor
    """

    vendor_na_dim_df = spark.sql(query)

    vendor_na_dim_df.write.format("delta").mode("overwrite").saveAsTable(
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

    source_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"  # Adjust if your source is named differently
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['vendor_na_dim']  # Make sure this key exists in your config

    insertVendorNaDimTask(
        spark=spark,
        logger=logger,
        source_db_name=source_db_name,
        target_db_name=target_db_name,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
