import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType
from pg_tw_fa_artemis.common import get_dbutils, get_spark


def insertDsSubsectorCostTask(spark, logger, hive_database, target_db_name, target_table):
    # Switch to the target Hive database
    spark.sql(f"USE {hive_database}")
    logger.info(f"Switched to database: {hive_database}")

    # SQL script exactly as provided (unchanged)
    query = f"""
    USE ${hivevar:database};

    Drop TABLE IF EXISTS ds_subsectorcosts_max
    ;

    CREATE TEMPORARY VIEW ds_subsectorcosts_max as
    SELECT
             subsector
           , voucher_id
           , MAX(mx.origin_zone)                    AS origin_zone
           , MAX(mx.destination_zone)               AS destination_zone
           , MAX(mx.customer_description)           AS customer_description
           , MAX(mx.carrier_description)            AS carrier_description
           , MAX(mx.actual_gi_date)                 AS actual_gi_date
           , MAX(mx.goods_receipt_posting_date)     AS goods_receipt_posting_date
           , MAX(mx.created_date)                   AS created_date
           , MAX(mx.ship_to_id)                     AS ship_to_id
           , MAX(mx.freight_auction)                AS freight_auction
           , MAX(mx.historical_data_structure_flag) AS historical_data_structure_flag
           , MAX(mx.destination_postal)             AS destination_postal
           , MAX(mx.service_tms_code)               AS service_tms_code
           , MAX(mx.customer_l8)                    AS customer_l8
           , MAX(mx.ship_to_description)            AS ship_to_description
           , MAX(mx.distance_per_load)              AS distance_per_load
           , MAX(mx.origin_sf)                      AS origin_sf
           , MAX(mx.destination_sf)                 AS destination_sf
           , MAX(mx.truckload_vs_intermodal_val)    AS truckload_vs_intermodal_val
    FROM
             (
                      SELECT
                               `load id` AS load_id
                             , case
                                        when `origin zone` = "SF_EDWARDSVILLE"
                                                 then "SF_ST_LOUIS"
                                                 else 'origin zone'
                               end as origin_zone
                             , case
                                        when `destination zone` = "SF_EDWARDSVILLE"
                                                 then "SF_ST_LOUIS"
                                                 else 'destination zone'
                               end                              as destination_zone
                             , `customer description`           as customer_description
                             , `carrier description`            as carrier_description
                             , max(`actual gi date`)            as actual_gi_date
                             , `country to`                     as country_to
                             , `subsector description`          as subsector
                             , `country to description`         as country_to_description
                             , `goods receipt posting date`     as goods_receipt_posting_date
                             , max(`created date`)              as created_date
                             , max(`ship to #`)                 as ship_to_id
                             , max(`ship to description`)       as ship_to_description
                             , `freight auction`                as freight_auction
                             , `historical data structure flag` as historical_data_structure_flag
                             , `destination postal`             as destination_postal
                             , `customer l8`                    as customer_l8
                             , truckload_vs_intermodal_val
                             , `distance per load`   as distance_per_load
                             , `service tms code`    as service_tms_code
                             , `voucher id`          as voucher_id
                             , min(`origin sf`)      as origin_sf
                             , min(`destination sf`) as destination_sf

                      FROM tfs

                      GROUP BY
                               `load id`
                             , `origin zone`
                             , `destination zone`
                             , `customer description`
                             , `carrier description`
                             , `country to`
                             , `subsector description`
                             , `country to description`
                             , `goods receipt posting date`
                             , `freight auction`
                             , `historical data structure flag`
                             , `destination postal`
                             , `customer l8`
                             , truckload_vs_intermodal_val
                             , `distance per load`
                             , `freight type`
                             , `service tms code`
                             , `voucher id`
             )
             mx
    GROUP BY
             subsector
           , voucher_id
    ;

    Drop TABLE IF EXISTS ds_subsectorcosts_noloadid_v13
    ;

    CREATE TEMPORARY VIEW ds_subsectorcosts_noloadid_v13 as
    SELECT
           load_id
         , voucher_id
         , service_tms_code
         , freight_type
         , country_to
         , dltl
         , dst
         , fuel
         , exm
         , lh_rules
         , weighted_average_rate_adjusted_long
         , accrual_cost_adjusted
         , accessorial_costs
         , weighted_average_rate_adjusted
         , distance_per_load_dec
         , unsourced_costs
         , line_haul_final
         , total_cost
         , origin_zone
         , customer_description
         , carrier_description
         , actual_gi_date
         , goods_receipt_posting_date
         , created_date
         , ship_to_id
         , freight_auction
         , historical_data_structure_flag
         , destination_postal
         , origin_sf
         , destination_sf
         , truckload_vs_intermodal_val
         , subsector_update AS subsector
         , fy
         , su_per_load
         , scs
         , ftp
         , tsc
         , flat
         , fa_a
         , steps_ratio_dec
         , cnc_costs
         , freight_auction_costs
         , country_to_description
         , customer_l8
         , distance_per_load
         , ship_to_description
         , destination_zone_new
    FROM
           (
                     SELECT
                               null_final.load_id                                                                    AS load_id
                             , null_final.voucher_id                                                                 AS voucher_id
                             , null_final.subsector                                                                  AS subsector
                             , null_final.service_tms_code                                                           AS service_tms_code
                             , null_final.freight_type                                                               AS freight_type
                             , null_final.country_to                                                                 AS country_to
                             , null_final.dltl                                                                       AS dltl
                             , null_final.dst                                                                        AS dst
                             , null_final.fuel                                                                       AS fuel
                             , null_final.exm                                                                        AS exm
                             , null_final.lh_rules                                                                   AS lh_rules
                             , null_final.weighted_average_rate_adjusted_long                                        AS weighted_average_rate_adjusted_long
                             , null_final.accrual_cost_adjusted                                                      AS accrual_cost_adjusted
                             , null_final.accessorial_costs                                                          AS accessorial_costs
                             , null_final.weighted_average_rate_adjusted                                             AS weighted_average_rate_adjusted
                             , null_final.distance_per_load_dec                                                      AS distance_per_load_dec
                             , null_final.unsourced_costs                                                            AS unsourced_costs
                             , null_final.line_haul_final                                                            AS line_haul_final
                             , null_final.total_cost                                                                 AS total_cost
                             , tfs_max.origin_zone                                                                   AS origin_zone
                             , tfs_max.destination_zone                                                              AS destination_zone
                             , tfs_max.customer_description                                                          AS customer_description
                             , tfs_max.carrier_description                                                           AS carrier_description
                             , tfs_max.actual_gi_date                                                                AS actual_gi_date
                             , tfs_max.goods_receipt_posting_date                                                    AS goods_receipt_posting_date
                             , tfs_max.created_date                                                                  AS created_date
                             , tfs_max.ship_to_id                                                                    AS ship_to_id
                             , tfs_max.freight_auction                                                               AS freight_auction
                             , tfs_max.historical_data_structure_flag                                                AS historical_data_structure_flag
                             , tfs_max.destination_postal                                                            AS destination_postal
                             , tfs_max.origin_sf                                                                     AS origin_sf
                             , tfs_max.destination_sf                                                                AS destination_sf
                             , tfs_max.truckload_vs_intermodal_val                                                   AS truckload_vs_intermodal_val
                             , regexp_replace(regexp_replace(null_final.subsector,' SUB-SECTOR',''),' SUBSECTOR','') AS subsector_update
                             , case
                                         WHEN MONTH(TO_DATE(actual_gi_date)) < 7
                                                   THEN CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date))-1 AS STRING),-2,2),SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date)) AS STRING),-2,2))
                                                   ELSE CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date)) AS STRING),-2,2),SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date))+1 AS STRING),-2,2))
                               END AS FY
                             , 0   AS su_per_load
                             , 0   AS scs
                             , 0   AS ftp
                             , 0   AS tsc
                             , 0   AS flat
                             , 0   AS fa_a
                             , 0   AS steps_ratio_dec
                             , 0   AS cnc_costs
                             , 0   AS freight_auction_costs
                             , ""  AS country_to_description
                             , ""  AS customer_l8
                             , 0   AS distance_per_load
                             , ""  AS ship_to_description
                             , ""  AS destination_zone_new
                     FROM
                               (
                                      SELECT *
                                           , (COALESCE(Line_Haul_Final,0) + COALESCE(accessorials,0)+ COALESCE(fuel,0) + COALESCE(Unsourced_Costs,0)) AS Total_Cost
                                      FROM
                                             (
                                                    SELECT *
                                                         , CASE
                                                                  WHEN LH_Rules = 0
                                                                         THEN COALESCE(dst,0)+COALESCE(exm,0)+COALESCE(dltl,0)
                                                                         ELSE 0
                                                       END AS Unsourced_Costs
                                                         , CASE
                                                                  WHEN LH_Rules = 1
                                                                         THEN COALESCE(dst,0)+COALESCE(exm,0)+COALESCE(dltl,0)
                                                                         ELSE 0
                                                       END AS Line_Haul_Final
                                                    FROM
                                                           (
                                                                  SELECT *
                                                                       , CASE
                                                                                WHEN load_id = "MISSING"
                                                                                       THEN "No Load ID"
                                                                                WHEN freight_type           = "Customer"
                                                                                       AND service_tms_code = "DSD"
                                                                                       THEN "No Load ID"
                                                                                ELSE `load id`
                                                                       END AS load_id
                                                                  FROM tfs
                                                           ) tfs
                                                    ) conditions1
                                      ) null_final
                               LEFT JOIN ds_subsectorcosts_max AS tfs_max
                                     ON null_final.voucher_id = tfs_max.voucher_id
           ) tb
    ;

    Drop TABLE IF EXISTS ds_subc_v14_max
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_max as
    SELECT
             `load id`                     AS load_id
           , CASE
                      WHEN `origin zone` = "SF_EDWARDSVILLE"
                               THEN "SF_ST_LOUIS"
                               ELSE `origin zone`
             END AS origin_zone_new
           , CASE
                      WHEN `destination zone` = "SF_EDWARDSVILLE"
                               THEN "SF_ST_LOUIS"
                               ELSE `destination zone`
             END                           AS destination_zone_new
           , `origin location id`          AS shipping_point_code
           , `customer description`        AS customer_description
           , `carrier description`         AS carrier_description
           , `country to`                  AS country_to
           , `subsector description`       AS subsector
           , `country to description`      AS country_to_description
           , `goods receipt posting date`  AS goods_receipt_posting_date
           , `freight auction`             AS freight_auction
           , `destination postal`          AS destination_postal
           , `customer l8`                 AS customer_l8
           , `truckload_vs_intermodal_val` AS truckload_vs_intermodal_val
           , `service tms code`            AS service_tms_code
           , MAX(`actual gi date`)         AS actual_gi_date
           , MAX(`created date`)           AS created_date
           , MAX
                      (
                               CASE
                                        WHEN `ship to #` = ""
                                                 THEN 0
                                                 ELSE CAST(TRIM(`ship to #`) AS INT)
                               END
                      )
             AS max_ship_to
           , MAX
                      (
                               CASE
                                        WHEN `historical data structure flag` = "-"
                                                 THEN 1
                                                 ELSE 0
                               END
                      )
                                      AS max_hist_num
           , MIN(`distance per load`) AS distance_per_load
           , MIN(`origin sf`)         AS origin_sf
           , MIN(`destination sf`)    AS destination_sf
    FROM tfs
    GROUP BY
             `load id`
           , `origin zone`
           , `destination zone`
           , `origin location id`
           , `customer description`
           , `carrier description`
           , `country to`
           , `subsector description`
           , `country to description`
           , `goods receipt posting date`
           , `freight auction`
           , `destination postal`
           , `customer l8`
           , `truckload_vs_intermodal_val`
           , `distance`
           , `freight type`
           , `service tms code`
    ;

    DROP TABLE IF EXISTS ds_subc_v14_ship_to
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_ship_to AS
    SELECT
                      (
                               CASE
                                        WHEN `ship to #` = ""
                                                 THEN 0
                                                 ELSE CAST(TRIM(`ship to #`) AS INT)
                               END
                      )
                                   AS ship_to
           , `ship to description` AS ship_to_description
    FROM tfs
    GROUP BY
             `ship to #`
           , `ship to description`
    ;

    DROP TABLE IF EXISTS ds_subc_v14_dest_id
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_dest_id AS
    SELECT
             CONCAT('0',load_id) AS new_load_id
           , destination_location_id
    FROM tac_tender_pg
    GROUP BY
             load_id
           , destination_location_id
    ;

    DROP TABLE IF EXISTS ds_subc_v14_dest_upd
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_dest_upd AS
    SELECT
             dest_postal_code AS destination_postal_update
           , dest_zone_val    AS destination_zone_update
    FROM tfs_null_dest_lkp
    GROUP BY
             dest_postal_code
           , dest_zone_val
    ;

    DROP TABLE IF EXISTS ds_subc_v14_orig_null
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_orig_null AS
    SELECT
             ship_point_code       AS shipping_point_code
           , origin_zone_null_name AS origin_zone_null
    FROM tfs_null_origin_lkp
    GROUP BY
             ship_point_code
           , origin_zone_null_name
    ;

    DROP TABLE IF EXISTS ds_subc_v14_tfs
    ;

    CREATE TEMPORARY VIEW ds_subc_v14_tfs AS
    SELECT
              tfs_dest_orig.load_id                                 AS load_id
            , tfs_dest_orig.subsector                               AS subsector
            , tfs_dest_orig.delivery_id                             AS delivery_id
            , tfs_dest_orig.tdc_val_code                            AS tdc_val_code
            , tfs_dest_orig.total_cost_usd                          AS total_cost_usd
            , tfs_dest_orig.subsector                               AS subsector
            , tfs_dest_orig.su                                      AS su
            , tfs_dest_orig.steps                                   AS steps
            , tfs_dest_orig.cost_ratio                              AS cost_ratio
            , tfs_dest_orig.accrual_cost                            AS accrual_cost
            , tfs_dest_orig.service_tms_code                        AS service_tms_code
            , tfs_dest_orig.freight_type                            AS freight_type
            , tfs_dest_orig.country_to                              AS country_to
            , tfs_dest_orig.distance_per_load                       AS distance_per_load
            , tfs_dest_orig.weighted_average_rate_old               AS weighted_average_rate_old
            , tfs_dest_orig.destination_zone_new                    AS destination_zone_new
            , case
                        when tfs_dest_orig.origin_zone is null
                                   then orig_null.origin_zone_null
                        else tfs_dest_orig.origin_zone
              end as origin_zone
    FROM
              ds_subc_v14_orig_null AS orig_null
              RIGHT JOIN
                         (
                                    SELECT
                                               dest_upd.destination_postal_update AS destination_postal_update
                                             , dest_upd.destination_zone_update   AS destination_zone_update
                                             , tfs.load_id                         AS load_id
                                             , tfs.destination_postal              AS destination_postal
                                             , tfs.actual_gi_date                  AS actual_gi_date
                                             , tfs.origin_zone                     AS origin_zone
                                             , tfs.destination_zone                AS destination_zone
                                             , tfs.delivery_id                     AS delivery_id
                                             , tfs.tdc_val_code                    AS tdc_val_code
                                             , tfs.total_cost_usd                  AS total_cost_usd
                                             , tfs.subsector                       AS subsector
                                             , tfs.su                              AS su
                                             , tfs.steps                           AS steps
                                             , tfs.cost_ratio                      AS cost_ratio
                                             , tfs.cost_grouping                   AS cost_grouping
                                             , tfs.accrual_cost                    AS accrual_cost
                                             , tfs.service_tms_code                AS service_tms_code
                                             , tfs.freight_type                    AS freight_type
                                             , tfs.country_to                      AS country_to
                                             , tfs.distance_per_load               AS distance_per_load
                                             , tfs.weighted_average_rate_old       AS weighted_average_rate_old
                                             , case
                                                        when to_date(tfs.actual_gi_date)                    >= '2016-11-01'
                                                                  then tfs.destination_zone
                                                        when tfs.destination_zone                  is null
                                                                  and dest_upd.destination_zone_update is not null
                                                                  then dest_upd.destination_zone_update
                                                        when tfs.destination_zone              is null
                                                                  and dest_upd.destination_zone_update is null
                                                                  then tfs.destination_postal
                                                                  else tfs.destination_zone
                                              end as destination_zone_new
                                    FROM
                                               ds_subc_v14_dest_upd AS dest_upd
                                               RIGHT JOIN
                                                          (
                                                                 SELECT 
                                                                        CASE
                                                                               WHEN `load id` is NULL
                                                                                      THEN "No Load ID"
                                                                               WHEN `load id` = ""
                                                                                      THEN "No Load ID"
                                                                                      ELSE `load id`
                                                                        END                  AS load_id
                                                                      , `destination postal` AS destination_postal
                                                                      , `actual gi date`     AS actual_gi_date
                                                                      , CASE
                                                                               WHEN `origin zone` = "SF_EDWARDSVILLE"
                                                                                      THEN "SF_ST_LOUIS"
                                                                                      ELSE `origin zone`
                                                                        END AS origin_zone
                                                                      , CASE
                                                                               WHEN `destination zone` = "SF_EDWARDSVILLE"
                                                                                      THEN "SF_ST_LOUIS"
                                                                                      ELSE `destination zone`
                                                                        END AS destination_zone
                                                                      , `delivery id #`       AS delivery_id
                                                                      , `tdc val code`        AS tdc_val_code
                                                                      , CAST(`total transportation cost usd` AS DECIMAL(30,8)) AS total_cost_usd
                                                                      , CASE
                                                                               WHEN `subsector description` is NULL
                                                                                      THEN "MISSING"
                                                                                      ELSE `subsector description`
                                                                        END AS subsector
                                                                      , CAST(`#su per load` AS DECIMAL(30,8)) AS su
                                                                      , CAST(`steps` AS DECIMAL(30,8)) AS steps
                                                                      , CAST(`total transportation cost local currency` AS DECIMAL(30,8))/CAST(`total transportation cost usd` AS DECIMAL(30,8)) AS cost_ratio
                                                                      , CASE
                                                                               WHEN `freight cost charge` IN ('FSUR','FU_S','FLTL','FFLT','FCHG','FUSU')
                                                                                      THEN 'FUEL'
                                                                               WHEN `freight cost charge` IN ('DST','CVYI','HJBT','KNIG','L2D','SCNN','UFLB','USXI','PGLI')
                                                                                      THEN 'DST'
                                                                               WHEN `freight cost charge` = 'EXM'
                                                                                      THEN 'EXM'
                                                                               WHEN `freight cost charge` = 'SCS'
                                                                                      THEN 'SCS'
                                                                               WHEN `freight cost charge` = 'FTP'
                                                                                      THEN 'FTP'
                                                                               WHEN `freight cost charge` = 'TSC'
                                                                                      THEN 'TSC'
                                                                               WHEN `freight cost charge` = 'DIST'
                                                                                      THEN 'DIST'
                                                                               WHEN `freight cost charge` = 'DLTL'
                                                                                      THEN 'DLTL'
                                                                               WHEN `freight cost charge` = 'FLAT'
                                                                                      THEN 'FLAT'
                                                                               WHEN `freight cost charge` = 'SPOT'
                                                                                      THEN 'SPOT'
                                                                               WHEN `freight cost charge` = 'FA_A'
                                                                                      THEN 'FA_A'
                                                                                      ELSE 'Accessorials'
                                                                      END AS cost_grouping
                                                                 FROM
                                                                        tfs
                                                          ) tfs
                                                          ON COALESCE(dest_upd.destination_postal_update, 'XX') = COALESCE(tfs.destination_postal, 'XX')
                                    )
                                    tfs_dest
                         )
                         tfs_dest_orig
                         LEFT JOIN tfs_hist_weight_avg_rate_lkp AS war_hist
                                   ON COALESCE(tfs_dest_orig.origin_zone, 'XX') = COALESCE(war_hist.origin_zone_name, 'XX')
                                   AND COALESCE(tfs_dest_orig.destination_zone_new, 'XX') = COALESCE(war_hist.dest_zone_val, 'XX')
    ;

    Drop table IF EXISTS ds_subc_v14_su
    ;

    Create TEMPORARY VIEW ds_subc_v14_su as
    SELECT
                 load_id
               , subsector
               , SUM(su) AS su
    FROM
    (
                              SELECT
                                           load_id
                                         , delivery_id
                                         , tdc_val_code
                                         , subsector
                                         , MAX(su) AS su
                              FROM
                                           ds_subc_v14_tfs
                              GROUP BY load_id
                                         , delivery_id
                                         , tdc_val_code
                                         , subsector
    ) as a
    GROUP BY
             load_id
           , subsector
    ;

    Drop table IF EXISTS ds_subc_v14_calcs
    ;

    Create TEMPORARY VIEW ds_subc_v14_calcs as
    SELECT
                 load_id
               , subsector
               , cost_grouping
               , SUM(steps) over(
                    partition by load_id) as steps_load
               , SUM(steps) over(
                    partition by load_id
                               , subsector) as steps_subsector
               , total_cost_usd
               , cost_ratio
               , accrual_cost
               , service_tms_code
               , country_to
               , distance_per_load
               , MAX(weighted_average_rate) AS weighted_average_rate
               , freight_type
    FROM
                 (
                              SELECT
                                           load_id
                                         , subsector
                                         , cost_grouping
                                         , SUM(steps) AS steps
                                         , SUM(total_cost_usd)        AS total_cost_usd
                                         , MAX(cost_ratio)            AS cost_ratio
                                         , SUM(accrual_cost)          AS accrual_cost
                                         , MAX(service_tms_code)      AS service_tms_code
                                         , MAX(country_to)            AS country_to
                                         , MIN(distance_per_load)     AS distance_per_load
                                         , MAX(weighted_average_rate) AS weighted_average_rate
                                         , MAX(freight_type)          AS freight_type
                              FROM
                                           ds_subc_v14_tfs
                              GROUP BY load_id
                                     , subsector
                                     , cost_grouping
    ) as a
    ;

    Drop table IF EXISTS ds_subc_v14_calcs_2
    ;

    Create TEMPORARY VIEW ds_subc_v14_calcs_2 as
    SELECT
             load_id
           , subsector
           , SUM(su)                         AS su
           , MAX(steps_load)                 AS steps_load
           , MAX(steps_subsector)            AS steps_subsector
           , MAX(steps_subsector/steps_load) AS steps_ratio
           , AVG(su)                         AS AVG_su_
           , MAX(cost_ratio)                 AS MAX_cost_ratio_
           , SUM(accrual_cost)               AS SUM_accrual_cost_
           , MAX(service_tms_code)           AS MAX_service_tms_code_
           , MAX(country_to)                 AS MAX_country_to_
           , MIN(distance_per_load)          AS MIN_distance_per_load_
           , MAX(weighted_average_rate)      AS MAX_weighted_average_rate_
           , MAX(steps_ratio)                AS MAX_steps_ratio_
           , MAX(freight_type)               AS MAX_freight_type_
    FROM (
           SELECT DISTINCT
                  cc.load_id
                , cc.subsector
                , su
                , steps_load
                , steps_subsector
                , (steps_subsector/steps_load) AS steps_ratio
                , cost_ratio
                , accrual_cost
                , service_tms_code
                , country_to
                , distance_per_load
                , weighted_average_rate
                , freight_type
           FROM ds_subc_v14_calcs AS cc
           JOIN ds_subc_v14_su as c_su
               ON cc.load_id = c_su.load_id
                 AND cc.subsector = c_su.subsector
    ) a
    GROUP BY
             load_id
           , subsector
    ;

    Drop table IF EXISTS ds_subsectorcosts_v14
    ;

    Create TEMPORARY VIEW ds_subsectorcosts_v14 as
    SELECT
           load_id
         , su_per_load
         , service_tms_code
         , country_to
         , freight_type
         , dltl
         , scs
         , ftp
         , dst
         , tsc
         , fuel
         , flat
         , exm
         , fa_a
         , lh_rules
         , weighted_average_rate_adjusted_long
         , accrual_cost_adjusted
         , accessorial_costs
         , weighted_average_rate_adjusted
         , distance_per_load_dec
         , steps_ratio_dec
         , unsourced_costs
         , line_haul_final
         , cnc_costs
         , freight_auction_costs
         , total_cost
         , customer_description
         , carrier_description
         , actual_gi_date
         , country_to_description
         , goods_receipt_posting_date
         , created_date
         , freight_auction
         , destination_postal
         , customer_l8
         , distance_per_load
         , ship_to_description
         , origin_sf
         , destination_sf
         , truckload_vs_intermodal_val
         , historical_data_structure_flag
         , ship_to_id
         , destination_zone_new
         , origin_zone_new AS origin_zone
         , fy
         , subsector_update AS subsector
         , ""               as voucher_id
    FROM
           (
                      SELECT
                                 table_179224308_1.origin_zone_null                    AS origin_zone_null
                               , table_179224308_2.destination_postal_update           AS destination_postal_update
                               , table_179224308_2.destination_zone_update             AS destination_zone_update
                               , table_179224308_2.load_id                             AS load_id
                               , table_179224308_2.subsector                           AS subsector
                               , table_179224308_2.su_per_load                         AS su_per_load
                               , table_179224308_2.service_tms_code                    AS service_tms_code
                               , table_179224308_2.country_to                          AS country_to
                               , table_179224308_2.freight_type                        AS freight_type
                               , table_179224308_2.dltl                                AS dltl
                               , table_179224308_2.scs                                 AS scs
                               , table_179224308_2.ftp                                 AS ftp
                               , table_179224308_2.dst                                 AS dst
                               , table_179224308_2.tsc                                 AS tsc
                               , table_179224308_2.fuel                                AS fuel
                               , table_179224308_2.flat                                AS flat
                               , table_179224308_2.exm                                 AS exm
                               , table_179224308_2.fa_a                                AS fa_a
                               , table_179224308_2.lh_rules                            AS lh_rules
                               , table_179224308_2.weighted_average_rate_adjusted_long AS weighted_average_rate_adjusted_long
                               , table_179224308_2.accrual_cost_adjusted               AS accrual_cost_adjusted
                               , table_179224308_2.accessorial_costs                   AS accessorial_costs
                               , table_179224308_2.weighted_average_rate_adjusted      AS weighted_average_rate_adjusted
                               , table_179224308_2.distance_per_load_dec               AS distance_per_load_dec
                               , table_179224308_2.steps_ratio_dec                     AS steps_ratio_dec
                               , table_179224308_2.unsourced_costs                     AS unsourced_costs
                               , table_179224308_2.line_haul_final                     AS line_haul_final
                               , table_179224308_2.cnc_costs                           AS cnc_costs
                               , table_179224308_2.freight_auction_costs               AS freight_auction_costs
                               , table_179224308_2.total_cost                          AS total_cost
                               , table_179224308_2.origin_zone                         AS origin_zone
                               , table_179224308_2.destination_zone                    AS destination_zone
                               , table_179224308_2.customer_description                AS customer_description
                               , table_179224308_2.carrier_description                 AS carrier_description
                               , table_179224308_2.actual_gi_date                      AS actual_gi_date
                               , table_179224308_2.country_to_description              AS country_to_description
                               , table_179224308_2.goods_receipt_posting_date          AS goods_receipt_posting_date
                               , table_179224308_2.created_date                        AS created_date
                               , table_179224308_2.freight_auction                     AS freight_auction
                               , table_179224308_2.destination_postal                  AS destination_postal
                               , table_179224308_2.customer_l8                         AS customer_l8
                               , table_179224308_2.distance_per_load                   AS distance_per_load
                               , table_179224308_2.ship_to_description                 AS ship_to_description
                               , table_179224308_2.shipping_point_code                 AS shipping_point_code
                               , table_179224308_2.origin_sf                           AS origin_sf
                               , table_179224308_2.destination_sf                      AS destination_sf
                               , table_179224308_2.truckload_vs_intermodal_val         AS truckload_vs_intermodal_val
                               , table_179224308_2.historical_data_structure_flag      AS historical_data_structure_flag
                               , table_179224308_2.ship_to_id                          AS ship_to_id
                               , table_179224308_2.destination_zone_new                AS destination_zone_new
                               , case
                                            when table_179224308_2.origin_zone is null
                                                       then table_179224308_1.origin_zone_null
                                            when table_179224308_2.origin_zone = ""
                                                       then table_179224308_1.origin_zone_null
                                                       else table_179224308_2.origin_zone
                                 end as origin_zone_new
                               , case
                                            when month(to_date(table_179224308_2.actual_gi_date)) < 7
                                                       then concat('FY',substr(cast(year(to_date(table_179224308_2.actual_gi_date))-1 as string),-2,2),substr(cast(year(to_date(table_179224308_2.actual_gi_date)) as string),-2,2))
                                                       else concat('FY',substr(cast(year(to_date(table_179224308_2.actual_gi_date)) as string),-2,2),substr(cast(year(to_date(table_179224308_2.actual_gi_date))+1 as string),-2,2))
                                 end as FY
                               , regexp_replace(regexp_replace(table_179224308_2.subsector,' SUB-SECTOR',''),' SUBSECTOR','') AS subsector_update
                      FROM
                               ds_subc_v14_orig_null AS table_179224308_1
                               RIGHT JOIN
                                          (
                                                     SELECT
                                                                table_370155390_1.destination_postal_update           AS destination_postal_update
                                                              , table_370155390_1.destination_zone_update             AS destination_zone_update
                                                              , table_370155390_2.load_id                             AS load_id
                                                              , table_370155390_2.subsector                           AS subsector
                                                              , table_370155390_2.su_per_load                         AS su_per_load
                                                              , table_370155390_2.service_tms_code                    AS service_tms_code
                                                              , table_370155390_2.country_to                          AS country_to
                                                              , table_370155390_2.freight_type                        AS freight_type
                                                              , table_370155390_2.dltl                                AS dltl
                                                              , table_370155390_2.scs                                 AS scs
                                                              , table_370155390_2.ftp                                 AS ftp
                                                              , table_370155390_2.dst                                 AS dst
                                                              , table_370155390_2.tsc                                 AS tsc
                                                              , table_370155390_2.fuel                                AS fuel
                                                              , table_370155390_2.flat                                AS flat
                                                              , table_370155390_2.exm                                 AS exm
                                                              , table_370155390_2.fa_a                                AS fa_a
                                                              , table_370155390_2.lh_rules                            AS lh_rules
                                                              , table_370155390_2.weighted_average_rate_adjusted_long AS weighted_average_rate_adjusted_long
                                                              , table_370155390_2.accrual_cost_adjusted               AS accrual_cost_adjusted
                                                              , table_370155390_2.accessorial_costs                   AS accessorial_costs
                                                              , table_370155390_2.weighted_average_rate_adjusted      AS weighted_average_rate_adjusted
                                                              , table_370155390_2.distance_per_load_dec               AS distance_per_load_dec
                                                              , table_370155390_2.steps_ratio_dec                     AS steps_ratio_dec
                                                              , table_370155390_2.unsourced_costs                     AS unsourced_costs
                                                              , table_370155390_2.line_haul_final                     AS line_haul_final
                                                              , table_370155390_2.cnc_costs                           AS cnc_costs
                                                              , table_370155390_2.freight_auction_costs               AS freight_auction_costs
                                                              , table_370155390_2.total_cost                          AS total_cost
                                                              , table_370155390_2.origin_zone                         AS origin_zone
                                                              , table_370155390_2.destination_zone                    AS destination_zone
                                                              , table_370155390_2.customer_description                AS customer_description
                                                              , table_370155390_2.carrier_description                 AS carrier_description
                                                              , table_370155390_2.actual_gi_date                      AS actual_gi_date
                                                              , table_370155390_2.country_to_description              AS country_to_description
                                                              , table_370155390_2.goods_receipt_posting_date          AS goods_receipt_posting_date
                                                              , table_370155390_2.created_date                        AS created_date
                                                              , table_370155390_2.freight_auction                     AS freight_auction
                                                              , table_370155390_2.destination_postal                  AS destination_postal
                                                              , table_370155390_2.customer_l8                         AS customer_l8
                                                              , table_370155390_2.distance_per_load                   AS distance_per_load
                                                              , table_370155390_2.ship_to_description                 AS ship_to_description
                                                              , table_370155390_2.shipping_point_code                 AS shipping_point_code
                                                              , table_370155390_2.origin_sf                           AS origin_sf
                                                              , table_370155390_2.destination_sf                      AS destination_sf
                                                              , table_370155390_2.truckload_vs_intermodal_val         AS truckload_vs_intermodal_val
                                                              , table_370155390_2.historical_data_structure_flag      AS historical_data_structure_flag
                                                              , table_370155390_2.ship_to_id                          AS ship_to_id
                                                              , case
                                                                           when to_date(table_370155390_2.actual_gi_date)                    >= '2017-07-01'
                                                                                      then table_370155390_2.destination_zone
                                                                           when table_370155390_2.destination_zone                      is null
                                                                                      and table_370155390_1.destination_zone_update is not null
                                                                                      then table_370155390_1.destination_zone_update
                                                                           when table_370155390_2.destination_zone                  is null
                                                                                      and table_370155390_1.destination_zone_update is null
                                                                                      then table_370155390_2.destination_postal
                                                                                      else table_370155390_2.destination_zone
                                                               end as destination_zone_new
                                                   FROM
                                                              ds_subc_v14_dest_upd AS table_370155390_1
                                                              RIGHT JOIN
                                                                         (
                                                                                   SELECT DISTINCT
                                                                                          `load id`           AS load_id
                                                                                        , `subsector`         AS subsector
                                                                                        , `su per load`       AS su_per_load
                                                                                        , `service tms code`  AS service_tms_code
                                                                                        , `country to`        AS country_to
                                                                                        , `freight type`      AS freight_type
                                                                                        , `dltl`              AS dltl
                                                                                        , `scs`               AS scs
                                                                                        , `ftp`               AS ftp
                                                                                        , `dst`               AS dst
                                                                                        , `tsc`               AS tsc
                                                                                        , `fuel`              AS fuel
                                                                                        , `flat`              AS flat
                                                                                        , `exm`               AS exm
                                                                                        , `fa_a`              AS fa_a
                                                                                        , `lh_rules`          AS lh_rules
                                                                                        , `weighted_average_rate_adjusted_long` AS weighted_average_rate_adjusted_long
                                                                                        , `accrual cost adjusted`               AS accrual_cost_adjusted
                                                                                        , `accessorial costs`                   AS accessorial_costs
                                                                                        , `weighted_average_rate_adjusted`      AS weighted_average_rate_adjusted
                                                                                        , `distance per load dec`               AS distance_per_load_dec
                                                                                        , `steps ratio dec`                     AS steps_ratio_dec
                                                                                        , `unsourced costs`                     AS unsourced_costs
                                                                                        , `line haul final`                     AS line_haul_final
                                                                                        , `cnc costs`                           AS cnc_costs
                                                                                        , `freight auction costs`               AS freight_auction_costs
                                                                                        , `total cost`                          AS total_cost
                                                                                        , `origin zone`                         AS origin_zone
                                                                                        , `destination zone`                    AS destination_zone
                                                                                        , `customer description`                AS customer_description
                                                                                        , `carrier description`                 AS carrier_description
                                                                                        , `actual gi date`                      AS actual_gi_date
                                                                                        , `country to description`              AS country_to_description
                                                                                        , `goods receipt posting date`          AS goods_receipt_posting_date
                                                                                        , `created date`                        AS created_date
                                                                                        , `freight auction`                     AS freight_auction
                                                                                        , `destination postal`                  AS destination_postal
                                                                                        , `customer l8`                         AS customer_l8
                                                                                        , `distance per load`                   AS distance_per_load
                                                                                        , `ship to description`                 AS ship_to_description
                                                                                        , `shipping point code`                 AS shipping_point_code
                                                                                        , `origin sf`                           AS origin_sf
                                                                                        , `destination sf`                      AS destination_sf
                                                                                        , `truckload_vs_intermodal_val`         AS truckload_vs_intermodal_val
                                                                                        , `historical data structure flag`      AS historical_data_structure_flag
                                                                                        , `ship to id`                          AS ship_to_id
                                                                                   FROM
                                                                                          tfs
                                                                         ) AS table_370155390_2
                                                                         ON
                                                                                    COALESCE(table_370155390_1.destination_postal_update, 'XX')=COALESCE(table_370155390_2.destination_postal, 'XX')
                                        )
                                        AS table_179224308_2
                                        ON
                                                   table_179224308_1.shipping_point_code=table_179224308_2.shipping_point_code
       )
       AS table_377828182
    ;

    Drop table IF EXISTS ds_subsectorcosts_v14_u
    ;

    Create TEMPORARY VIEW ds_subsectorcosts_v14_u as
    select * from ds_subsectorcosts_v14
    UNION ALL
    select
           load_id
         , su_per_load
         , service_tms_code
         , country_to
         , freight_type
         , dltl
         , scs
         , ftp
         , dst
         , tsc
         , fuel
         , flat
         , exm
         , fa_a
         , lh_rules
         , weighted_average_rate_adjusted_long
         , accrual_cost_adjusted
         , accessorial_costs
         , weighted_average_rate_adjusted
         , distance_per_load_dec
         , steps_ratio_dec
         , unsourced_costs
         , line_haul_final
         , cnc_costs
         , freight_auction_costs
         , total_cost
         , customer_description
         , carrier_description
         , actual_gi_date
         , country_to_description
         , goods_receipt_posting_date
         , created_date
         , freight_auction
         , destination_postal
         , customer_l8
         , cast(distance_per_load as STRING) as distance_per_load
         , ship_to_description
         , origin_sf
         , destination_sf
         , truckload_vs_intermodal_val
         , historical_data_structure_flag
         , ship_to_id
         , destination_zone_new
         , origin_zone
         , fy
         , subsector
         , cast(voucher_id as STRING) as voucher_id
    from
           ds_subsectorcosts_noloadid_v13
    ;

    INSERT OVERWRITE table tfs_subsector_cost_star
    select *
         , case
                  when origin_zone = 'origin zone'
                         then 'Undefined'
                         else origin_zone
           end as origin_zone_v1
         , case
                  when destination_zone_new = ''
                         then 'Undefined'
                  when destination_zone_new is NULL
                         then 'Undefined'
                  when destination_zone_new = 'NULL'
                         then 'Undefined'
                         else destination_zone_new
           end as destination_zone_v1
    from
           ds_subsectorcosts_v14_u
    ;
    """

    # Split the SQL text into individual commands (by semicolon)
    commands = [cmd.strip() for cmd in query.strip().split(";") if cmd.strip()]

    # Execute each command sequentially
    for i, cmd in enumerate(commands, 1):
        logger.info(f"Executing command {i}: {cmd.splitlines()[0]}")
        spark.sql(cmd)

    logger.info(
        "Data has been successfully loaded into {}.{}".format(target_db_name, target_table)
    )

    return 0


def main():
    spark = get_dbutils().notebook().getSparkSession()  # Adjust as needed to obtain SparkSession
    dbutils = get_dbutils()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # For configuration loading; adjust as needed
    config = Configuration.load_for_default_environment(__file__, dbutils)

    hive_database = config['hive_database']
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['tfs_subsector_cost_star']

    insertDsSubsectorCostTask(
        spark=spark,
        logger=logger,
        hive_database=hive_database,
        target_db_name=target_db_name,
        target_table=target_table
    )


if __name__ == "__main__":
    main()
