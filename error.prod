import logging
import sys
#from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import SparkSession
#from pg_tw_fa_transfix.common import get_dbutils, get_spark

def insertProdNaDimTask(spark, logger, g11_db_name, target_db_name, target_table):
  query = f"""
    WITH
    tmp_attribute_values_103_104 AS (
      SELECT 
        b.material_number, 
        CONCAT_WS('', b.category)   AS category, 
        CONCAT_WS('', b.sector)     AS sector, 
        CONCAT_WS('', b.sub_sector) AS sub_sector,
        CONCAT_WS('', b.segment)    AS segment,
        CONCAT_WS('', b.brand)      AS brand,
        CONCAT_WS('', b.form_globl) AS form_globl,
        CONCAT_WS('', b.form_det)   AS form_det,
        CONCAT_WS('', b.sub_brand)  AS sub_brand,
        CONCAT_WS('', b.fl_sc_det)  AS fl_sc_det,
        CONCAT_WS('', b.unit_sz_as) AS unit_sz_as,
        CONCAT_WS('', b.cb1)        AS cb1,
        CONCAT_WS('', b.prim_pk_tp) AS prim_pk_tp,
        CONCAT_WS('', b.tdc_val)    AS tdc_val,
        CONCAT_WS('', b.cb2)        AS cb2,
        CONCAT_WS('', b.cb3)        AS cb3,
        CONCAT_WS('', b.cb4)        AS cb4,
        CONCAT_WS('', b.cb5)        AS cb5,
        CONCAT_WS('', b.cb6)        AS cb6,
        CONCAT_WS('', b.cb7)        AS cb7,
        CONCAT_WS('', b.csu_ind)    AS csu_ind,
        CONCAT_WS('', b.cust_purp)  AS cust_purp,
        CONCAT_WS('', b.cust_type)  AS cust_type,
        CONCAT_WS('', b.evday_avai) AS evday_avai,
        CONCAT_WS('', b.pto_prod)   AS pto_prod,
        CONCAT_WS('', b.gp1)        AS gp1,
        CONCAT_WS('', b.gp2)        AS gp2,
        CONCAT_WS('', b.gp3)        AS gp3,
        CONCAT_WS('', b.gp4)        AS gp4,
        CONCAT_WS('', b.gp5)        AS gp5,
        CONCAT_WS('', b.count)      AS count,
        CONCAT_WS('', b.art_brand)  AS art_brand,
        CONCAT_WS('', b.ref_nref)   AS ref_nref,
        CONCAT_WS('', b.collection) AS collection,
        CONCAT_WS('', b.prod_size)  AS prod_size,
        CONCAT_WS('', b.gender)     AS gender,
        CONCAT_WS('', b.shade_code) AS shade_code,
        CONCAT_WS('', b.cons_pk_sz) AS cons_pk_sz,
        CONCAT_WS('', b.outlet)     AS outlet,
        CONCAT_WS('', b.label)      AS label,
        CONCAT_WS('', b.clr_shd_nm) AS clr_shd_nm,
        CONCAT_WS('', b.cos_col)    AS cos_col,
        CONCAT_WS('', b.frq_of_use) AS frq_of_use,
        CONCAT_WS('', b.usage)      AS usage
      FROM (
        SELECT 
          material_number,
          COLLECT_LIST(a.GROUP_MAP['CATEGORY'])   AS category,
          COLLECT_LIST(a.GROUP_MAP['SECTOR'])       AS sector,
          COLLECT_LIST(a.GROUP_MAP['SUB_SECTOR'])   AS sub_sector,
          COLLECT_LIST(a.GROUP_MAP['SEGMENT'])      AS segment,
          COLLECT_LIST(a.GROUP_MAP['BRAND'])        AS brand,
          COLLECT_LIST(a.GROUP_MAP['FORM_GLOBL'])   AS form_globl,
          COLLECT_LIST(a.GROUP_MAP['FORM_DET'])      AS form_det,
          COLLECT_LIST(a.GROUP_MAP['SUB_BRAND'])     AS sub_brand,
          COLLECT_LIST(a.GROUP_MAP['FL_SC_DET'])     AS fl_sc_det,
          COLLECT_LIST(a.GROUP_MAP['UNIT_SZ_AS'])    AS unit_sz_as,
          COLLECT_LIST(a.GROUP_MAP['CB1'])           AS cb1,
          COLLECT_LIST(a.GROUP_MAP['PRIM_PK_TP'])      AS prim_pk_tp,
          COLLECT_LIST(a.GROUP_MAP['TDC_VAL'])       AS tdc_val,
          COLLECT_LIST(a.GROUP_MAP['CB2'])           AS cb2,
          COLLECT_LIST(a.GROUP_MAP['CB3'])           AS cb3,
          COLLECT_LIST(a.GROUP_MAP['CB4'])           AS cb4,
          COLLECT_LIST(a.GROUP_MAP['CB5'])           AS cb5,
          COLLECT_LIST(a.GROUP_MAP['CB6'])           AS cb6,
          COLLECT_LIST(a.GROUP_MAP['CB7'])           AS cb7,
          COLLECT_LIST(a.GROUP_MAP['CSU_IND'])       AS csu_ind,
          COLLECT_LIST(a.GROUP_MAP['CUST_PURP'])     AS cust_purp,
          COLLECT_LIST(a.GROUP_MAP['CUST_TYPE'])     AS cust_type,
          COLLECT_LIST(a.GROUP_MAP['EVDAY_AVAI'])    AS evday_avai,
          COLLECT_LIST(a.GROUP_MAP['PTO_PROD'])      AS pto_prod,
          COLLECT_LIST(a.GROUP_MAP['GP1'])           AS gp1,
          COLLECT_LIST(a.GROUP_MAP['GP2'])           AS gp2,
          COLLECT_LIST(a.GROUP_MAP['GP3'])           AS gp3,
          COLLECT_LIST(a.GROUP_MAP['GP4'])           AS gp4,
          COLLECT_LIST(a.GROUP_MAP['GP5'])           AS gp5,
          COLLECT_LIST(a.GROUP_MAP['COUNT'])         AS count,
          COLLECT_LIST(a.GROUP_MAP['ART_BRAND'])     AS art_brand,
          COLLECT_LIST(a.GROUP_MAP['REF_NREF'])      AS ref_nref,
          COLLECT_LIST(a.GROUP_MAP['COLLECTION'])    AS collection,
          COLLECT_LIST(a.GROUP_MAP['PROD_SIZE'])     AS prod_size,
          COLLECT_LIST(a.GROUP_MAP['GENDER'])        AS gender,
          COLLECT_LIST(a.GROUP_MAP['SHADE_CODE'])    AS shade_code,
          COLLECT_LIST(a.GROUP_MAP['CONS_PK_SZ'])    AS cons_pk_sz,
          COLLECT_LIST(a.GROUP_MAP['OUTLET'])        AS outlet,
          COLLECT_LIST(a.GROUP_MAP['LABEL'])         AS label,
          COLLECT_LIST(a.GROUP_MAP['CLR_SHD_NM'])    AS clr_shd_nm,
          COLLECT_LIST(a.GROUP_MAP['COS_COL'])       AS cos_col,
          COLLECT_LIST(a.GROUP_MAP['FRQ_OF_USE'])    AS frq_of_use,
          COLLECT_LIST(a.GROUP_MAP['USAGE'])         AS usage
        FROM (
          SELECT
            104_1.MATNR AS material_number,
            MAP(104_1.ZATTRTYPID,
              CASE
                WHEN 104_1.ZATTRTYPID = 'UNIT_SZ_AS' THEN 104_1.ZATTRVALID
                ELSE 103_1.ZATTRVALDC
              END
            ) AS GROUP_MAP
          FROM cdl_ps_prod.silver_sap_h1p_104.ZTXXPT0104 AS 104_1
          LEFT JOIN cdl_ps_prod.silver_sap_h1p_104.ZTXXPT0103 AS 103_1
            ON 103_1.ZATTRTYPID = 104_1.ZATTRTYPID
            AND 103_1.ZATTRVALID = 104_1.ZATTRVALID
          WHERE 104_1.ZATTRTYPID IN (
            'SECTOR','SUB_SECTOR','CATEGORY','SEGMENT','BRAND','FORM_GLOBL','FORM_DET',
            'SUB_BRAND','FL_SC_DET','UNIT_SZ_AS','CB1','PRIM_PK_TP','TDC_VAL','CB2','CB3',
            'CB4','CB5','CB6','CB7','CSU_IND','CUST_PURP','CUST_TYPE','EVDAY_AVAI','PTO_PROD',
            'GP1','GP2','GP3','GP4','GP5','COUNT','ART_BRAND','REF_NREF','COLLECTION','PROD_SIZE',
            'GENDER','SHADE_CODE','CONS_PK_SZ','OUTLET','LABEL','CLR_SHD_NM','COS_COL','FRQ_OF_USE','USAGE'
          )
        ) a
        GROUP BY a.material_number
      ) b
    ),
    tmp_marm_ean11 AS (
      SELECT 
        b.material_number, 
        CONCAT_WS('', b.it) AS it, 
        CONCAT_WS('', b.sw) AS sw, 
        CONCAT_WS('', b.cs) AS cs, 
        CONCAT_WS('', b.sp) AS sp, 
        CONCAT_WS('', b.b2) AS b2, 
        CONCAT_WS('', b.p2) AS p2, 
        CONCAT_WS('', b.c2) AS c2
      FROM (
        SELECT 
          material_number,
          COLLECT_LIST(a.GROUP_MAP['IT']) AS it,
          COLLECT_LIST(a.GROUP_MAP['SW']) AS sw,
          COLLECT_LIST(a.GROUP_MAP['CS']) AS cs,
          COLLECT_LIST(a.GROUP_MAP['SP']) AS sp,
          COLLECT_LIST(a.GROUP_MAP['B2']) AS b2,
          COLLECT_LIST(a.GROUP_MAP['P2']) AS p2,
          COLLECT_LIST(a.GROUP_MAP['C2']) AS c2
        FROM (
          SELECT
            MATNR AS material_number,
            MAP(MEINH, EAN11) AS GROUP_MAP
          FROM {g11_db_name}.MARM
          WHERE MEINH IN ('IT','SW','CS','SP','B2','P2','C2')
        ) a
        GROUP BY a.material_number
      ) b
    ),
    tmp_ztxxptmelf_stage_life_cycle AS (
      SELECT 
        b.material_number, 
        CONCAT_WS('', b.us) AS us, 
        CONCAT_WS('', b.ca) AS ca
      FROM (
        SELECT 
          material_number,
          COLLECT_LIST(a.GROUP_MAP['US']) AS us,
          COLLECT_LIST(a.GROUP_MAP['CA']) AS ca
        FROM (
          SELECT
            MATNR AS material_number,
            MAP(ZLCS, ZSTAGE4) AS GROUP_MAP
          FROM cdl_ps_prod.silver_sap_h1p_104.ZTXXPTMELF
          WHERE ZLCS IN ('US', 'CA')
        ) a
        GROUP BY a.material_number
      ) b
    ),
    tmp_ztxxptatva_attribute AS (
      SELECT 
        b.material_number, 
        CONCAT_WS('', b.a902) AS a902,
        CONCAT_WS('', b.a081) AS a081,
        CONCAT_WS('', b.a121) AS a121,
        CONCAT_WS('', b.a122) AS a122,
        CONCAT_WS('', b.a123) AS a123,
        CONCAT_WS('', b.a011) AS a011,
        CONCAT_WS('', b.a013) AS a013,
        CONCAT_WS('', b.a080) AS a080,
        CONCAT_WS('', b.a125) AS a125,
        CONCAT_WS('', b.a143) AS a143,
        CONCAT_WS('', b.a145) AS a145,
        CONCAT_WS('', b.a903) AS a903,
        CONCAT_WS('', b.a904) AS a904,
        CONCAT_WS('', b.a161) AS a161,
        CONCAT_WS('', b.a162) AS a162,
        CONCAT_WS('', b.a163) AS a163,
        CONCAT_WS('', b.a098) AS a098,
        CONCAT_WS('', b.a156) AS a156,
        CONCAT_WS('', b.a157) AS a157,
        CONCAT_WS('', b.a158) AS a158,
        CONCAT_WS('', b.a159) AS a159,
        CONCAT_WS('', b.a160) AS a160,
        CONCAT_WS('', b.a164) AS a164,
        CONCAT_WS('', b.a193) AS a193,
        CONCAT_WS('', b.a194) AS a194,
        CONCAT_WS('', b.a195) AS a195,
        CONCAT_WS('', b.a196) AS a196,
        CONCAT_WS('', b.a197) AS a197
      FROM (
        SELECT 
          material_number,
          COLLECT_LIST(a.GROUP_MAP['902']) AS a902,
          COLLECT_LIST(a.GROUP_MAP['081']) AS a081,
          COLLECT_LIST(a.GROUP_MAP['121']) AS a121,
          COLLECT_LIST(a.GROUP_MAP['122']) AS a122,
          COLLECT_LIST(a.GROUP_MAP['123']) AS a123,
          COLLECT_LIST(a.GROUP_MAP['011']) AS a011,
          COLLECT_LIST(a.GROUP_MAP['013']) AS a013,
          COLLECT_LIST(a.GROUP_MAP['080']) AS a080,
          COLLECT_LIST(a.GROUP_MAP['125']) AS a125,
          COLLECT_LIST(a.GROUP_MAP['143']) AS a143,
          COLLECT_LIST(a.GROUP_MAP['145']) AS a145,
          COLLECT_LIST(a.GROUP_MAP['903']) AS a903,
          COLLECT_LIST(a.GROUP_MAP['904']) AS a904,
          COLLECT_LIST(a.GROUP_MAP['161']) AS a161,
          COLLECT_LIST(a.GROUP_MAP['162']) AS a162,
          COLLECT_LIST(a.GROUP_MAP['163']) AS a163,
          COLLECT_LIST(a.GROUP_MAP['098']) AS a098,
          COLLECT_LIST(a.GROUP_MAP['156']) AS a156,
          COLLECT_LIST(a.GROUP_MAP['157']) AS a157,
          COLLECT_LIST(a.GROUP_MAP['158']) AS a158,
          COLLECT_LIST(a.GROUP_MAP['159']) AS a159,
          COLLECT_LIST(a.GROUP_MAP['160']) AS a160,
          COLLECT_LIST(a.GROUP_MAP['164']) AS a164,
          COLLECT_LIST(a.GROUP_MAP['193']) AS a193,
          COLLECT_LIST(a.GROUP_MAP['194']) AS a194,
          COLLECT_LIST(a.GROUP_MAP['195']) AS a195,
          COLLECT_LIST(a.GROUP_MAP['196']) AS a196,
          COLLECT_LIST(a.GROUP_MAP['197']) AS a197
        FROM (
          SELECT
            MATNR AS material_number,
            MAP(ZPATRNR, ZPATRVAL) AS GROUP_MAP
          FROM cdl_ps_prod.silver_sap_h1p_104.ZTXXPTATVA
          WHERE ZPATRNR IN ('902','081','121','122','123','011','013','080','125','143','145','903','904','161','162','163','098','156','157','158','159','160','164','193','194','195','196','197')
        ) a
        GROUP BY a.material_number
      ) b
    ),
    tmp_marm_distinct_material AS (
      SELECT DISTINCT MATNR AS material_number 
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
    ),
    tmp_marm_it AS (
      SELECT
        MATNR AS material_number,
        UMREZ AS numerator_for_conversion_to_base_units_of_measure,
        UMREN AS denominator_for_conversion_to_base_units_of_measure
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
      WHERE MEINH = 'IT' 
        AND EXISTS (
          SELECT 1 FROM tmp_marm_distinct_material AS mat 
          WHERE cdl_ps_prod.silver_sap_h1p_104.MARM.MATNR = mat.material_number
        )
    ),
    tmp_marm_sw AS (
      SELECT
        MATNR AS material_number,
        UMREZ AS numerator_for_conversion_to_base_units_of_measure,
        UMREN AS denominator_for_conversion_to_base_units_of_measure
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
      WHERE MEINH = 'SW' 
        AND EXISTS (
          SELECT 1 FROM tmp_marm_distinct_material AS mat 
          WHERE cdl_ps_prod.silver_sap_h1p_104.MARM.MATNR = mat.material_number
        )
    ),
    tmp_marm_gu AS (
      SELECT
        MATNR AS material_number,
        UMREZ AS numerator_for_conversion_to_base_units_of_measure,
        UMREN AS denominator_for_conversion_to_base_units_of_measure
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
      WHERE MEINH = 'GU' 
        AND EXISTS (
          SELECT 1 FROM tmp_marm_distinct_material AS mat 
          WHERE cdl_ps_prod.silver_sap_h1p_104.MARM.MATNR = mat.material_number
        )
    ),
    tmp_marm_cs AS (
      SELECT
        MATNR AS material_number,
        UMREZ AS numerator_for_conversion_to_base_units_of_measure,
        UMREN AS denominator_for_conversion_to_base_units_of_measure
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
      WHERE MEINH = 'CS' 
        AND EXISTS (
          SELECT 1 FROM tmp_marm_distinct_material AS mat 
          WHERE cdl_ps_prod.silver_sap_h1p_104.MARM.MATNR = mat.material_number
        )
    ),
    tmp_marm_su AS (
      SELECT
        MATNR AS material_number,
        UMREZ AS numerator_for_conversion_to_base_units_of_measure,
        UMREN AS denominator_for_conversion_to_base_units_of_measure
      FROM cdl_ps_prod.silver_sap_h1p_104.MARM
      WHERE MEINH = 'SU' 
        AND EXISTS (
          SELECT 1 FROM tmp_marm_distinct_material AS mat 
          WHERE cdl_ps_prod.silver_sap_h1p_104.MARM.MATNR = mat.material_number
        )
    ),
    tmp_umrez_gu_it_sw_cs AS (
      SELECT
        base.material_number,
        gu.numerator_for_conversion_to_base_units_of_measure   AS gu_umrez,
        gu.denominator_for_conversion_to_base_units_of_measure AS gu_umren,
        it.numerator_for_conversion_to_base_units_of_measure   AS it_umrez,
        it.denominator_for_conversion_to_base_units_of_measure AS it_umren,
        sw.numerator_for_conversion_to_base_units_of_measure   AS sw_umrez,
        sw.denominator_for_conversion_to_base_units_of_measure AS sw_umren,
        cs.numerator_for_conversion_to_base_units_of_measure   AS cs_umrez,
        cs.denominator_for_conversion_to_base_units_of_measure AS cs_umren,
        su.numerator_for_conversion_to_base_units_of_measure   AS su_umrez,
        su.denominator_for_conversion_to_base_units_of_measure AS su_umren,
        CAST(CASE 
          WHEN COALESCE(it.denominator_for_conversion_to_base_units_of_measure, 0) * COALESCE(gu.numerator_for_conversion_to_base_units_of_measure,0) > 0
            THEN (it.numerator_for_conversion_to_base_units_of_measure * gu.denominator_for_conversion_to_base_units_of_measure) / (it.denominator_for_conversion_to_base_units_of_measure * gu.numerator_for_conversion_to_base_units_of_measure)
          ELSE 0
        END AS INT) AS gu_it,
        CAST(CASE 
          WHEN COALESCE(sw.denominator_for_conversion_to_base_units_of_measure, 0) * COALESCE(it.numerator_for_conversion_to_base_units_of_measure,0) > 0
            THEN (sw.numerator_for_conversion_to_base_units_of_measure * it.denominator_for_conversion_to_base_units_of_measure) / (sw.denominator_for_conversion_to_base_units_of_measure * it.numerator_for_conversion_to_base_units_of_measure)
          ELSE 0
        END AS INT) AS it_sw,
        CAST(CASE 
          WHEN COALESCE(cs.denominator_for_conversion_to_base_units_of_measure, 0) * COALESCE(sw.numerator_for_conversion_to_base_units_of_measure,0) > 0
            THEN (cs.numerator_for_conversion_to_base_units_of_measure * sw.denominator_for_conversion_to_base_units_of_measure) / (cs.denominator_for_conversion_to_base_units_of_measure * sw.numerator_for_conversion_to_base_units_of_measure)
          ELSE 0
        END AS INT) AS sw_cs,
        CAST(CASE 
          WHEN COALESCE(cs.denominator_for_conversion_to_base_units_of_measure, 0) * COALESCE(it.numerator_for_conversion_to_base_units_of_measure,0) > 0
            THEN (cs.numerator_for_conversion_to_base_units_of_measure * it.denominator_for_conversion_to_base_units_of_measure) / (cs.denominator_for_conversion_to_base_units_of_measure * it.numerator_for_conversion_to_base_units_of_measure)
          ELSE 0
        END AS INT) AS it_cs,
        CAST(CASE 
          WHEN COALESCE(su.numerator_for_conversion_to_base_units_of_measure, 0) <> 0
            THEN (su.denominator_for_conversion_to_base_units_of_measure / su.numerator_for_conversion_to_base_units_of_measure)
          ELSE 0
        END AS DECIMAL(10,4)) AS stat_value
      FROM tmp_marm_distinct_material AS base
      LEFT JOIN tmp_marm_it AS it ON base.material_number = it.material_number
      LEFT JOIN tmp_marm_sw AS sw ON base.material_number = sw.material_number
      LEFT JOIN tmp_marm_gu AS gu ON base.material_number = gu.material_number
      LEFT JOIN tmp_marm_cs AS cs ON base.material_number = cs.material_number
      LEFT JOIN tmp_marm_su AS su ON base.material_number = su.material_number
    ),
    tmp_ztxxpvclsf_1 AS (
      SELECT
        cabn.MANDT AS client,
        cabn.ATINN AS internal_characteristic,
        cabn.ADZHL AS internal_counter_for_archiving_objects_via_engin_chg_mgmt,
        cabn.ATNAM AS characteristic_name
      FROM (SELECT ZPICKID FROM dbPrdMasterDataG11}.ZTXXPTCPIK WHERE ZPICDES = 'LANGUAGE ON PACK') AS pik
      JOIN cdl_ps_prod.silver_sap_h1p_104.CABN AS cabn
        ON pik.ZPICKID = cabn.ATNAM
    ),
    tmp_ztxxpvclsf_ausp AS (
      SELECT
        ausp.OBJEK AS key_of_object_to_be_classified,
        ausp.MANDT AS client,
        ausp.ATINN AS internal_characteristic,
        ausp.ADZHL AS internal_counter_for_archiving_objects_via_engin_chg_mgmt,
        ausp.KLART AS class_type,
        ausp.MAFID AS indicator_object_class,
        ausp.ATZHL AS characteristic_value_counter,
        ausp.ATWRT AS characteristic_value
      FROM cdl_ps_prod.silver_sap_h1p_104.AUSP AS ausp
      WHERE EXISTS (
        SELECT 1 FROM tmp_ztxxpvclsf_1 AS cabn
        WHERE ausp.MANDT = cabn.client
          AND ausp.ATINN = cabn.internal_characteristic
          AND ausp.ADZHL = cabn.internal_counter_for_archiving_objects_via_engin_chg_mgmt
      )
    ),
    tmp_ztxxpvclsf_kssk AS (
      SELECT
        kssk.MANDT AS client,
        kssk.OBJEK AS key_of_object_to_be_classified,
        kssk.ADZHL AS internal_counter_for_archiving_objects_via_engin_chg_mgmt,
        kssk.MAFID AS indicator_object_class,
        kssk.KLART AS class_type,
        kssk.CLINT AS internal_class_number
      FROM cdl_ps_prod.silver_sap_h1p_104.KSSK AS kssk
      WHERE EXISTS (
        SELECT 1 FROM tmp_ztxxpvclsf_ausp AS ausp
        WHERE kssk.MANDT = ausp.client
          AND kssk.OBJEK = ausp.key_of_object_to_be_classified
          AND kssk.ADZHL = ausp.internal_counter_for_archiving_objects_via_engin_chg_mgmt
          AND kssk.MAFID = ausp.indicator_object_class
          AND kssk.KLART = ausp.class_type
      )
    ),
    tmp_ztxxpvclsf AS (
      SELECT
        ausp.key_of_object_to_be_classified,
        cabn.characteristic_name,
        concat_ws(",", collect_list(ausp.characteristic_value)) AS characteristic_value
      FROM tmp_ztxxpvclsf_1 AS cabn
      JOIN tmp_ztxxpvclsf_ausp AS ausp
        ON ausp.client = cabn.client
        AND ausp.internal_characteristic = cabn.internal_characteristic
        AND ausp.internal_counter_for_archiving_objects_via_engin_chg_mgmt = cabn.internal_counter_for_archiving_objects_via_engin_chg_mgmt
      JOIN cdl_ps_prod.silver_sap_h1p_104.KSML AS ksml
        ON ksml.MANDT = ausp.client
        AND ksml.IMERK = ausp.internal_characteristic
        AND ksml.ADZHL = ausp.internal_counter_for_archiving_objects_via_engin_chg_mgmt
      JOIN cdl_ps_prod.silver_sap_h1p_104.KLAH AS klah
        ON klah.MANDT = ausp.client
        AND klah.KLART = ausp.class_type
        AND klah.CLINT = ksml.CLINT
        AND klah.KLART = ksml.KLART
      JOIN tmp_ztxxpvclsf_kssk AS kssk
        ON kssk.client = ausp.client
        AND kssk.key_of_object_to_be_classified = ausp.key_of_object_to_be_classified
        AND kssk.internal_counter_for_archiving_objects_via_engin_chg_mgmt = ausp.internal_counter_for_archiving_objects_via_engin_chg_mgmt
        AND kssk.indicator_object_class = ausp.indicator_object_class
        AND kssk.class_type = ausp.class_type
        AND kssk.internal_class_number = klah.CLINT
      GROUP BY ausp.key_of_object_to_be_classified, cabn.characteristic_name
    ),
    tmp_gocoa_planning_unit AS (
      SELECT 
        ptwfreq.material_number_1,
        ptwfreq.owning_region,
        ptwfreq.current_wf_status,
        ptwconf.HIER_ID,
        ztxxptnass.ZNODEID_P AS node_code_1,
        ztxxptnass.ZNODEID_C AS node_code_2
      FROM (
        SELECT DISTINCT
          ZMATNR AS material_number_1,
          ZREGION AS owning_region,
          ZREQSTAT AS current_wf_status
        FROM cdl_ps_prod.silver_sap_h1p_104.ZTXX_PTWFREQ
        WHERE ZREQSTAT = 'POSTED'
      ) AS ptwfreq
      JOIN (
        SELECT 
          ZFIELD1 AS ZREGION,
          ZFIELD4 AS HIER_ID
        FROM cdl_ps_prod.silver_sap_h1p_104.ZTXX_PTWFCONFIG
        WHERE ZSCREEN = 'ATTRIBUTES'
          AND ZFIELDNAME = 'HIERARCHY_STUB'
      ) AS ptwconf
        ON ptwfreq.owning_region = ptwconf.ZREGION
      JOIN cdl_ps_prod.silver_sap_h1p_104.ZTXXPTNASS AS ztxxptnass
        ON ztxxptnass.ZHIER = ptwconf.HIER_ID
           AND LPAD(ztxxptnass.ZNODEID_C, 18, '0') = ptwfreq.material_number_1
    )
    SELECT 
      SUBSTR(mara.MATNR, -8) AS material_number,
      UPPER(makt.MAKTG) AS `Matl Description`,
      av.sector,
      av.sub_sector,
      av.category,
      av.segment,
      av.brand,
      av.form_globl,
      av.form_det,
      av.sub_brand,
      av.fl_sc_det,
      av.unit_sz_as,
      av.cb1,
      av.prim_pk_tp,
      av.tdc_val,
      clsf.characteristic_value AS language_on_pack,
      ztxxptatva.a902 AS proposed_prod_plant,
      ztxxptatva.a081 AS coo,
      umrez.gu_it,
      umrez.it_sw,
      umrez.sw_cs,
      umrez.it_cs,
      marm.it AS it_barcode,
      marm.sw AS sw_barcode,
      marm.cs AS ca_barcode,
      ptw1.material_sub_type AS material_sub_type,
      CASE
        WHEN LENGTH(ptw2.material_number_2) > 1 THEN ptw2.material_number_2
        ELSE NULL
      END AS copy_from_product,
      ztxxptatva.a081 AS coo_at,
      pu.node_code_1 AS gocoa_planning_unit,
      umrez.stat_value,
      ztxxptmelf.us AS us_start_ship_date,
      ztxxptmelf.ca AS canada_start_ship_date,
      av.cb2,
      av.cb3,
      av.cb4,
      av.cb5,
      av.cb6,
      av.cb7,
      av.csu_ind,
      av.cust_purp,
      av.cust_type,
      av.evday_avai,
      av.pto_prod,
      av.gp1,
      av.gp2,
      av.gp3,
      av.gp4,
      av.gp5,
      av.count,
      av.art_brand,
      av.ref_nref,
      av.collection,
      av.prod_size,
      av.gender,
      av.shade_code,
      av.cons_pk_sz,
      av.outlet,
      av.label,
      av.clr_shd_nm,
      av.cos_col,
      av.frq_of_use,
      av.usage,
      ztxxptatva.a121 AS standard_first_order_date,
      ztxxptatva.a122 AS standard_last_order_date,
      ztxxptatva.a123 AS standard_first_ship_date,
      ztxxptatva.a011 AS heat_protect,
      ztxxptatva.a013 AS freeze_protect,
      ztxxptatva.a080 AS dual_coo_flag,
      ztxxptatva.a125 AS dea_flag,
      ztxxptatva.a143 AS shelf_pack_indicator,
      ztxxptatva.a145 AS order_lead_time,
      ztxxptatva.a903 AS approbation_group,
      ztxxptatva.a904 AS oc_brush_id,
      marm.sp AS sp_barcode,
      marm.b2 AS b2_barcode,
      marm.p2 AS p2_barcode,
      marm.c2 AS c2_barcode,
      ztxxptatva.a161 AS start_of_production_date,
      ztxxptatva.a162 AS dc_shipped_to_location,
      ztxxptatva.a163 AS halb_code,
      ztxxptatva.a098 AS conversion_type,
      ztxxptatva.a156 AS first_delivery_date,
      ztxxptatva.a157 AS fd_first_order_date,
      ztxxptatva.a159 AS fd_first_ship_date,
      ztxxptatva.a158 AS fd_last_order_date,
      ztxxptatva.a160 AS fd_last_ship_date,
      ztxxptatva.a164 AS predecessor_code,
      ztxxptatva.a193 AS proposed_prod_plant_193,
      ztxxptatva.a194 AS proposed_prod_plant_194,
      ztxxptatva.a195 AS proposed_prod_plant_195,
      ztxxptatva.a196 AS proposed_prod_plant_196,
      ztxxptatva.a197 AS proposed_prod_plant_197,
      CASE 
        WHEN LENGTH(CONCAT(av.cust_purp, av.cust_type)) > 0 THEN 'Y' 
        ELSE 'N' 
      END AS customization_flag,
      CASE 
        WHEN LENGTH(regexp_extract(LOWER(av.tdc_val), '(pgp)', 0)) > 0 THEN 'P' 
        ELSE 'R' 
      END AS pgp_retail_flag    
    FROM cdl_ps_prod.silver_sap_h1p_104.MARA AS mara
    LEFT JOIN cdl_ps_prod.silver_sap_h1p_104.MAKT AS makt
      ON mara.MATNR = makt.MATNR
      AND makt.SPRAS = 'E'
    LEFT JOIN tmp_attribute_values_103_104 AS av
      ON mara.MATNR = av.material_number
    LEFT JOIN tmp_marm_ean11 AS marm
      ON marm.material_number = mara.MATNR
    LEFT JOIN tmp_ztxxptmelf_stage_life_cycle AS ztxxptmelf
      ON mara.MATNR = ztxxptmelf.material_number
    LEFT JOIN tmp_ztxxptatva_attribute AS ztxxptatva
      ON ztxxptatva.material_number = mara.MATNR
    LEFT JOIN tmp_umrez_gu_it_sw_cs AS umrez
      ON umrez.material_number = mara.MATNR
    LEFT JOIN (
      SELECT DISTINCT 
        ZMATNR AS material_number_1,
        ZSUBTYPE AS material_sub_type
      FROM cdl_ps_prod.silver_sap_h1p_104.ZTXX_PTWFREQ
      WHERE LENGTH(ZSUBTYPE) > 1
    ) AS ptw1
      ON mara.MATNR = ptw1.material_number_1
    LEFT JOIN (
      SELECT DISTINCT 
        ZMATNR AS material_number_1,
        ZCOPY AS material_number_2
      FROM {g11_db_name}.ZTXX_PTWFREQ
      WHERE LENGTH(ZCOPY) > 1
    ) AS ptw2
      ON mara.MATNR = ptw2.material_number_1
    LEFT JOIN tmp_ztxxpvclsf AS clsf
      ON mara.MATNR = clsf.key_of_object_to_be_classified
    LEFT JOIN tmp_gocoa_planning_unit AS pu
      ON mara.MATNR = pu.material_number_1;
    """



    df=spark.sql(query)
    display(df)
    print(df.count())



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

    #config = Configuration.load_for_default_environment(__file__, dbutils)

    g11_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"  
    schema = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = f"{config['tables']['prod1_na_dim']}"

    insertVendorNaDimTask(
        spark=spark,
        logger=logger,
        g11_db_name=g11_db_name,
        target_db_name=schema,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()



