import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pg_tw_fa_artemis.common import get_dbutils, get_spark


def insertDsTacDetailsPgCalcTask(spark, logger, hive_database, target_db_name, target_table):
    # Set the current Hive database
    spark.sql(f"USE {hive_database}")
    logger.info(f"Switched to database: {hive_database}")

    # SQL script (unchanged) taken directly from your attached file
    query = f"""
    USE ${hivevar:database};

    CREATE temp view operational_tariff AS
    select 
    `source`.`lane_origin_zone_code` as `lane origin zone`,
    `source`.`lane_dstn_zone_id` as `lane destination zone`,
    `source`.`origin_corp_code` as `origin corporate id 2`,
    `source`.`dest_zip_code` as `destination zip`,
    `source`.`dest_loc_desc` as `dest location description`,
    `source`.`dest_corp_code` as `dest corporate id2`,
    `source`.`exclude_from_optimization_flag` as `exclude from optimization`,
    `source`.`exclude_from_cst_flag` as `exclude from cst`,
    `source`.`tariff_id` as `tariff number`,
    `source`.`carrier_id` as `carrier id`,
    `source`.`carrier_name` as `carrier description`,
    `source`.`service_code` as `service`,
    `source`.`service_desc` as `service description`,
    `source`.`cea_profile_code` as `cea profile id`,
    `source`.`train_operating_day_qty` as `number of train op days`,
    `source`.`train_day_of_week_list` as `train days of week`,
    `source`.`transit_time_val` as `transit time`,
    `source`.`charge_code` as `charge code`,
    `source`.`charge_desc` as `charge description`,
    `source`.`mile_contract_rate` as `rate`,
    `source`.`payment_crncy_code` as `payment currency`,
    `source`.`rate_unit_code` as `rating unit`,
    `source`.`base_charge_amt` as `base charge`,
    `source`.`min_charge_amt` as `minimum charge`,
    `source`.`rate_eff_date` as `rate effective date`,
    `source`.`rate_exp_date` as `rate exp date`,
    `source`.`min_weight_qty` as `minimum weight`,
    `source`.`min_floor_postn_cnt` as `minimum floor positions`,
    `source`.`max_floor_postn_cnt` as `maximum floor positions`,
    `source`.`max_weight_qty` as `maximum weight`,
    `source`.`auto_accept_tender_override_flag` as `auto-accept tender override`,
    `source`.`freight_auction_eligibility_code` as `freight auction eligibility`,
    `source`.`alloc_type_code` as `allocation type`,
    `source`.`alloc_profile_val` as `allocation profile basis`,
    `source`.`award_rate` as `awards`,
    `source`.`weekly_alloc_pct` as `percent weekly allocation`,
    `source`.`mon_min_load_qty` as `mon min (loads)`,
    `source`.`mon_max_load_qty` as `mon max (loads)`,
    `source`.`tue_min_load_qty` as `tue min (loads)`,
    `source`.`tue_max_load_qty` as `tue max (loads)`,
    `source`.`wed_min_load_qty` as `wed min (loads)`,
    `source`.`wed_max_load_qty` as `wed max (loads)`,
    `source`.`thu_min_load_qty` as `thu min (loads)`,
    `source`.`thu_max_load_qty` as `thu max (loads)`,
    `source`.`fri_min_load_qty` as `fri min (loads)`,
    `source`.`fri_max_load_qty` as `fri max (loads)`,
    `source`.`sat_min_load_qty` as `sat min (loads)`,
    `source`.`sat_max_load_qty` as `sat max (loads)`,
    `source`.`sun_min_load_qty` as `sun min (loads)`,
    `source`.`sun_max_load_qty` as `sun max (loads)`,
    `source`.`dlvry_schedule_code` as `delivery schedule id`,
    `source`.`equip_type_code` as `equipment type`,
    `source`.`status_code` as `status`,
    `source`.`rate_code` as `rate code`,
    `source`.`service_grade_val` as `service grade`,
    `source`.`tariff_desc` as `tariff id`,
    `source`.`max_no_of_shpmt_cnt` as `maximum no of shipments`,
    `source`.`cust_id` as `customer id`,
    `source`.`cust_desc` as `customer description`,
    `source`.`equip_code` as `equipment code`,
    `source`.`resource_project_code` as `resource project id`,
    `source`.`report_date` as `report date`,
    case
      when report_date is not null and week_day_code !='Mon'
      Then week_begin_date
      else report_date
    end as TAC_report_date
    from dp_trans_vsblt_bw.operational_tariff_star as source 
    left join tac_calendar_lkp as lkp
    on lkp.actual_goods_issue_date = source.`report_date` 
    ;

    CREATE temp view operational_tariff_stage_CR16 as
    select
      `lane origin zone`,
      `lane destination zone`,
      case
        when (`origin corporate id 2` is null or `origin corporate id 2` = '') then `lane origin zone`
        else `origin corporate id 2`
      end as `origin corporate id 2`,
      case
        when (`dest corporate id2` is null or `dest corporate id2` = '') then `lane destination zone`
        else `dest corporate id2`
      end as `dest corporate id2`,
      `exclude from optimization`,
      `exclude from cst`,
      `tariff number`,
      `carrier id`,
      `carrier description`,
      `service`,
      `service description`,
      `cea profile id`,
      `number of train op days`,
      `train days of week`,
      `transit time`,
      `charge code`,
      `charge description`,
      `rate`,
      `payment currency`,
      `rating unit`,
      `base charge`,
      `minimum charge`,
      `rate effective date`,
      `rate exp date`,
      `minimum weight`,
      `minimum floor positions`,
      `maximum floor positions`,
      `maximum weight`,
      `auto-accept tender override`,
      `freight auction eligibility`,
      `allocation type`,
      `allocation profile basis`,
      `awards`,
      `percent weekly allocation`,
      `mon min (loads)`,
      `mon max (loads)`,
      `tue min (loads)`,
      `tue max (loads)`,
      `wed min (loads)`,
      `wed max (loads)`,
      `thu min (loads)`,
      `thu max (loads)`,
      `fri min (loads)`,
      `fri max (loads)`,
      `sat min (loads)`,
      `sat max (loads)`,
      `sun min (loads)`,
      `sun max (loads)`,
      `delivery schedule id`,
      `equipment type`,
      `status`,
      `rate code`,
      `service grade`,
      `tariff id`,
      `maximum no of shipments`,
      `customer id`,
      `customer description`,
      `equipment code`,
      `resource project id`,
      `report date`,
       TAC_report_date
    from
      operational_tariff
    ;
      
    CREATE temp view operational_tariff_Zone_Awards_CR16 as
    select `report date`, 
           `lane origin zone`, 
           `lane destination zone`, 
            replace(`origin corporate id 2`,'&','') as `origin corporate id 2`, 
            replace(`dest corporate id2`,'&','') as `dest corporate id2`, 
           `carrier id`, 
           `service`, 
           Max(awards) as awards,
           TAC_report_date
    from operational_tariff_stage_CR16
    group by TAC_report_date, 
            `report date`,
            `lane origin zone`, 
            `lane destination zone`, 
            `origin corporate id 2`, 
            `dest corporate id2`, 
            `carrier id`, 
            `service`
    ;
                
    CREATE temp view operational_tariff_SF_Awards_CR16 as
    select  concat (rtrim(left(`origin corporate id 2`,20)),'-',left(`dest corporate id2`,20)) as `SF Lane`,
            TAC_report_date, 
            `report date`, 
            `origin corporate id 2`, 
            `dest corporate id2`, 
            `carrier id`, 
            `service`, 
            SUM(awards) as SF_awards 
    from operational_tariff_Zone_Awards_CR16
    group by `SF Lane`, 
              `report date`, 
              TAC_report_date, 
              `origin corporate id 2`, 
              `dest corporate id2`, 
              `carrier id`, 
              `service`
    ;
            
    CREATE TEMPORARY VIEW tmp_tac_tender_pg_1_CR16 AS
    SELECT
           actual_ship_date,
           tac_day_of_year,
           workweekday,
           week_begin_date,
           monthtype445,
           calendar_year,
           month_date,
           week_number,
           day_of_week,
           load_id,
           origin_sf,
           origin_location_id,
           origin_zone,
           destination_sf,
           destination_zone,
           destination_location_id,
           customer_id,
           customer_id_description,
           carrier_id,
           carrier_description,
           mode,
           tender_event_type_code,
           tender_reason_code,
           tender_event_date,
           tender_event_time,
           tender_event_date_a_time,
           transportation_planning_point,
           departure_country_code,
           sap_original_shipdate,
           original_tendered_ship_date,
           day_of_the_week_of_original_tendered_shipdate,
           actual_ship_time,
           actual_ship_date_and_time,
           sold_to_n,
           no_of_days_in_week,
           no_of_days_in_month,
           scac,
           carrier_mode_description,
           tariff_id,
           schedule_id,
           tender_event_type_description,
           tender_acceptance_key,
           tender_reason_code_description,
           scheduled_date,
           scheduled_time,
           average_awarded_weekly_volume,
           daily_award,
           day_of_the_week,
           allocation_type,
           sun_max_loads,
           mon_max_loads,
           tue_max_loads,
           wed_max_loads,
           thu_max_loads,
           fri_max_loads,
           sat_max_loads,
           gbu_per_shipping_site,
           shipping_conditions,
           postal_code_raw_tms,
           postal_code_final_stop,
           country_from,
           country_to,
           freight_auction_flag,
           true_fa_flag,
           freight_type,
           operational_freight_type,
           pre_tms_upgrade_flag,
           data_structure_version,
           primary_carrier_flag,
           tendered_back_to_primary_carrier_with_no_fa_adjustment,
           tendered_back_to_primary_carrier_with__fa_adjustment,
           tender_accepted_loads_with_no_fa,
           tender_accepted_loads_with_fa,
           tender_rejected_loads,
           previous_tender_date_a_time,
           time_between_tender_events,
           canceled_due_to_no_response,
           customer,
           customer_level_1_id,
           customer_level_1_description,
           customer_level_2_id,
           customer_level_2_description,
           customer_level_3_id,
           customer_level_3_description,
           customer_level_4_id,
           customer_level_4_description,
           customer_level_5_id,
           customer_level_5_description,
           customer_level_6_id,
           customer_level_6_description,
           customer_level_7_id,
           customer_level_7_description,
           customer_level_8_id,
           customer_level_8_description,
           customer_level_9_id,
           customer_level_9_description,
           customer_level_10_id,
           customer_level_10_description,
           customer_level_11_id,
           customer_level_11_description,
           customer_level_12_id,
           customer_level_12_description,
           actual_carrier_id,
           actual_carrier_description,
           actual_carrier_total_transportation_cost_usd as actual_carrier_total_transportation_cost_usd_temp,
           linehaul as linehaul_temp,
           incremental_fa as incremental_fa_temp,
           cnc_carrier_mix as cnc_carrier_mix_temp,
           unsourced as unsourced_temp,
           fuel as fuel_temp,
           accessorial as accessorial_temp,
           concat(carrier_id,'-',`tms_service_code`, '-', `load_id`, '-', scotts_magical_field_of_dreams) AS MattKey,
           to_date(`tender_event_date_a_time`) AS tender_date_time_type,
           TO_DATE(current_timestamp()) AS `current_date`
    FROM
           tmp_tac_tender_pg_1_CR16
    ;
    
    REFRESH TABLE tac_response2021;
    
    CREATE TEMPORARY VIEW TAC_Response_Tender_union  AS
    select * from tac_response2021
    union all
    select * from ds_tac_tender_pg_tenders_only_CR16
    ;
    
    CREATE TEMPORARY VIEW TAC_Detail_Only_temp
    AS
    select a.*, b.SF_awards, b.TAC_report_date
    from TAC_Response_Tender_union a
    left join operational_tariff_SF_Awards_CR16 b
    on a.carrier_id = substring(b.`carrier id`, 3,8)
    and a.week_begin_date = b.TAC_report_date
    and a.SF_Lane = b.`SF Lane`
    and a.tms_service_code = b.service
    ;
    
    INSERT OVERWRITE TABLE TAC_DETAIL_ONLY_STAR
    select *,
    case
      when origin_zone = 'origin zone' then 'Undefined'
      else origin_zone
    end as origin_zone_v1,
    case
      when destination_zone_new = '' then 'Undefined'
      when destination_zone_new is NULL then 'Undefined'
      when destination_zone_new = 'NULL' then 'Undefined'
      else destination_zone_new
    end as destination_zone_v1
    from TAC_DETAIL_ONLY_STAR
    ;
    
    REFRESH TABLE TAC_DETAIL_ONLY_STAR;
    
    CREATE TEMPORARY VIEW Tac_GroupbySFOnly_tmp_1
    AS 
    select *,
    case
        when allocation_type ='Daily' then
        case
             when sun_max_loads <= Sunday_total_count 
             then sun_max_loads
             else Sunday_total_count
        end 
    end as Sunday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when mon_max_loads <= Monday_total_count 
             then mon_max_loads
             else Monday_total_count
        end 
    end as Monday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when tue_max_loads <= Tuesday_total_count 
             then tue_max_loads
             else Tuesday_total_count
        end 
    end as Tuesday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when wed_max_loads <= Wednesday_total_count 
             then wed_max_loads
             else Wednesday_total_count
        end 
    end as Wednesday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when thu_max_loads <= Thursday_total_count 
             then thu_max_loads
             else Thursday_total_count
        end 
    end as Thursday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when fri_max_loads <= Friday_total_count 
             then fri_max_loads
             else Friday_total_count
        end 
    end as Friday_expected_count,
    case
        when allocation_type ='Daily' then
        case
             when sat_max_loads <= Saturday_total_count 
             then sat_max_loads
             else Saturday_total_count
        end 
    end as Saturday_expected_count
    from 
    (select SF_Lane, week_begin_date, carrier_id, tms_service_code,
    min(MattKey) as Matt_Key,
    sum(SF_accept_count) as SF_accept_count,
    sum(SF_total_count) as SF_total_count,
    sum(SF_reject_count) as SF_reject_count,
    max(SF_awards) as SF_Awards,
    min(allocation_type) as allocation_type,
    max(sun_max_loads) as sun_max_loads,
    max(mon_max_loads) as mon_max_loads,
    max(tue_max_loads) as tue_max_loads,
    max(wed_max_loads) as wed_max_loads,
    max(thu_max_loads) as thu_max_loads,
    max(fri_max_loads) as fri_max_loads,
    max(sat_max_loads) as sat_max_loads,
    sum(Sunday_total_count) as Sunday_total_count,
    sum(Monday_total_count) as Monday_total_count,
    sum(Tuesday_total_count) as Tuesday_total_count,
    sum(Wednesday_total_count) as Wednesday_total_count,
    sum(Thursday_total_count) as Thursday_total_count,
    sum(Friday_total_count) as Friday_total_count,
    sum(Saturday_total_count) as Saturday_total_count
    from TAC_DETAIL_ONLY_STAR
    group by SF_Lane, week_begin_date,carrier_id, tms_service_code)
    ;
    
    CREATE TEMPORARY VIEW Tac_GroupbySFOnly_tmp_2 AS 
    select SF_Lane, week_begin_date, carrier_id, tms_service_code, Matt_Key, SF_accept_count, SF_total_count, SF_reject_count,
    SF_Awards, allocation_type, sun_max_loads, mon_max_loads, tue_max_loads, wed_max_loads, thu_max_loads,
    fri_max_loads, sat_max_loads, Sunday_total_count, Monday_total_count, Tuesday_total_count, Wednesday_total_count,
    Thursday_total_count, Friday_total_count, Saturday_total_count, Sunday_expected_count, Monday_expected_count,
    Tuesday_expected_count, Wednesday_expected_count, Thursday_expected_count, Friday_expected_count, Saturday_expected_count,
    expected_volume 
    from 
    (select *, 
    case
       when allocation_type ='Daily' then daily_count
       when allocation_type !='Daily' then
        case 
          when SF_Awards > SF_total_count then SF_total_count
          when SF_Awards < SF_total_count then SF_Awards
          else SF_total_count
        end  
    end as expected_volume
    from 
    (select (COALESCE(SUM(Sunday_expected_count),0) + COALESCE(SUM(Monday_expected_count),0) + COALESCE(SUM(Tuesday_expected_count),0) + COALESCE(SUM(Wednesday_expected_count),0) + COALESCE(SUM(Thursday_expected_count),0) + COALESCE(SUM(Friday_expected_count),0) + COALESCE(SUM(Saturday_expected_count),0)) as daily_count, *
    from Tac_GroupbySFOnly_tmp_1
    group by SF_Lane, week_begin_date, carrier_id, tms_service_code, Matt_Key, SF_accept_count, SF_total_count,
    SF_reject_count, SF_Awards, allocation_type, sun_max_loads, mon_max_loads, tue_max_loads, wed_max_loads,
    thu_max_loads, fri_max_loads, sat_max_loads, Sunday_total_count, Monday_total_count, Tuesday_total_count,
    Wednesday_total_count, Thursday_total_count, Friday_total_count, Saturday_total_count, Sunday_expected_count,
    Monday_expected_count, Tuesday_expected_count, Wednesday_expected_count, Thursday_expected_count,
    Friday_expected_count, Saturday_expected_count)
    )
    ;
    
    INSERT OVERWRITE TABLE TAC_GROUPBYSFONLY_STAR
    select *, 
    case
      when allocation_type !='Daily' then
        case
         when 0 > (expected_volume-SF_accept_count) then 0
         else (expected_volume-SF_accept_count) 
        end  
    end as rejects_below_award  
    from Tac_GroupbySFOnly_tmp_2
    ;
    
    REFRESH TABLE TAC_GROUPBYSFONLY_STAR;
    
    INSERT OVERWRITE TABLE TAC_DETAILANDSUMMARY_STAR
    select a.SF_Lane, a.carrier_description, a.tms_service_code, a.customer, b.SF_accept_count, b.SF_total_count, b.SF_Awards,
    b.expected_volume, b.rejects_below_award, a.actual_ship_date, a.load_id, a.tender_event_date_a_time,
    a.tender_event_type_description, a.accept_count, a.allocation_type, a.cassell_key, a.tac_day_of_year, a.workweekday,
    a.week_begin_date, a.calendar_year, a.month_date, a.week_number, a.day_of_week, a.origin_sf, a.origin_location_id,
    a.origin_zone, a.destination_sf, a.destination_zone, a.destination_location_id, a.customer_id, a.customer_id_description,
    a.carrier_id, a.mode, a.tender_event_type_code, a.tender_reason_code, a.tender_event_date, a.tender_event_time,
    a.transportation_planning_point, a.departure_country_code, a.sap_original_shipdate, a.original_tendered_ship_date,
    a.day_of_the_week_of_original_tendered_shipdate, a.actual_ship_time, a.actual_ship_date_and_time, a.sold_to_n,
    a.scac, a.carrier_mode_description, a.tariff_id, a.schedule_id, a.tender_acceptance_key, a.tender_reason_code_description,
    a.scheduled_date, a.scheduled_time, a.average_awarded_weekly_volume, a.daily_award, a.day_of_the_week, a.sun_max_loads,
    a.mon_max_loads, a.tue_max_loads, a.wed_max_loads, a.thu_max_loads, a.fri_max_loads, a.sat_max_loads,
    a.gbu_per_shipping_site, a.shipping_conditions, a.postal_code_raw_tms, a.postal_code_final_stop, a.country_from,
    a.country_to, a.true_fa_flag, a.freight_type, a.operational_freight_type, a.pre_tms_upgrade_flag, a.data_structure_version,
    a.primary_carrier_flag, a.tendered_back_to_primary_carrier_with_no_fa_adjustment,
    a.tendered_back_to_primary_carrier_with__fa_adjustment, a.tender_accepted_loads_with_no_fa,
    a.tender_accepted_loads_with_fa, a.tender_rejected_loads, a.previous_tender_date_a_time, a.time_between_tender_events,
    a.canceled_due_to_no_response, a.customer_level_1_id, a.customer_level_1_description, a.customer_level_2_id,
    a.customer_level_2_description, a.customer_level_3_id, a.customer_level_3_description, a.customer_level_4_id,
    a.customer_level_4_description, a.customer_level_5_id, a.customer_level_5_description, a.customer_level_6_id,
    a.customer_level_6_description, a.customer_level_7_id, a.customer_level_7_description, a.customer_level_8_id,
    a.customer_level_8_description, a.customer_level_9_id, a.customer_level_9_description, a.customer_level_10_id,
    a.customer_level_10_description, a.customer_level_11_id, a.customer_level_11_description, a.customer_level_12_id,
    a.customer_level_12_description, a.actual_carrier_id, a.actual_carrier_description, a.appliances, a.baby_care,
    a.chemicals, a.fabric_care, a.family_care, a.fem_care, a.hair_care, a.home_care, a.oral_care, a.phc, a.shave_care,
    a.skin_a_personal_care, a.other, a.tfts_load_tmstp, a.load_from_file, a.bd_mod_tmstp, a.historical_data_structure_flag,
    a.tender_date_time_type, a.state_province, a.Monday_total_count, a.Thursday_total_count, a.Friday_total_count,
    a.Sunday_total_count, a.Wednesday_total_count, a.Tuesday_total_count, a.Saturday_total_count, a.total_count,
    a.actual_ship_date_format, a.destination_zip, a.lane, a.lane_new, a.historical_lane, a.customer_specific_lane,
    a.calendar_year_week_tac, a.`Why_row_not_counted_toward_award`, a.reject_count, a.accessorial,
    a.actual_carrier_total_transportation_cost_usd, a.cnc_carrier_mix, a.fuel, a.incremental_fa, a.linehaul,
    a.unsourced, b.SF_reject_count,
    case
      when b.expected_volume is not null then b.SF_Awards
      Else null
    end as SF_Awards_1row
    from TAC_DETAIL_ONLY_STAR a
    left join TAC_GROUPBYSFONLY_STAR b
    on a.MattKey = b.Matt_Key
    and b.SF_Awards > 0.02
    ;
    
    """

    # Split the SQL script into individual commands using semicolon as the delimiter
    commands = [cmd.strip() for cmd in query.strip().split(";") if cmd.strip()]
    
    # Execute each command in sequence
    for i, cmd in enumerate(commands, 1):
        logger.info(f"Executing command {i}: {cmd.splitlines()[0]}")
        spark.sql(cmd)
    
    logger.info("Data has been successfully loaded into the target table")
    return 0


def main():
    spark = get_spark()
    dbutils = get_dbutils()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    config = Configuration.load_for_default_environment(__file__, dbutils)

    hive_database = config['hive_database']
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['tac_details_pg_calc']

    insertDsTacDetailsPgCalcTask(
        spark=spark,
        logger=logger,
        hive_database=hive_database,
        target_db_name=target_db_name,
        target_table=target_table
    )


if __name__ == "__main__":
    main()
