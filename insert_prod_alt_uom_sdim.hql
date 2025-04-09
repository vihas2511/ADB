set mapreduce.map.java.opts=-Xmx3276m;
set mapreduce.map.memory.mb=4096;
set mapreduce.job.queuename=${hivevar:hiveQueueName};

--The code is almost the same as in Hydra
INSERT OVERWRITE TABLE ${hivevar:hiveApplicationDB}.prod_alt_uom_sdim 
SELECT marm_out.prod_id, marm_out.alt_uom 
     -- casting as decimal(18,12) to get output number decimal(24,18).
     , IF(marm_out.denominator_alt_uom_buom_val> 0, cast(marm_out.numerator_alt_uom_buom_val as decimal(18,12))/marm_out.denominator_alt_uom_buom_val, NULL) as alt_uom_buom_factor
     , IF(marm_out.numerator_alt_uom_buom_val> 0  , cast(marm_out.denominator_alt_uom_buom_val as decimal(18,12))/marm_out.numerator_alt_uom_buom_val, NULL) as buom_alt_uom_factor
     , marm_out.numerator_alt_uom_buom_val  
     , marm_out.denominator_alt_uom_buom_val
  FROM (SELECT marm.matnr as prod_id, marm.meinh as alt_uom
             , marm.umren as denominator_alt_uom_buom_val
             , marm.umrez as numerator_alt_uom_buom_val
         FROM ${hivevar:hiveG11DB}.marm marm
         JOIN (
				  SELECT mara.matnr as prod_id
                 FROM ${hivevar:hiveG11DB}.mara mara
                WHERE mara.mtart = 'FERT'  --we filter only FERT products (Finished Products) for Artemis
                  AND mara.simp_chng_type_code not in ('I','U','D') --Selecting all the records except 'D','I','U'             
UNION
select prod_id
				from(SELECT mara.matnr as prod_id, ROW_NUMBER() over(partition by mara.matnr order by mara.bd_mod_utc_time_stamp desc )as rnk
             				FROM ${hivevar:hiveG11DB}.mara mara
				where  mara.simp_chng_type_code in ('I','U') and mara.mtart = 'FERT' ) --MARM duplication fix for I\U
                where rnk=1 --FIltering duplicates and selecting U/I based on max(timestamp)		
              ) ap
           ON marm.matnr = ap.prod_id
         WHERE marm.simp_chng_type_code != 'D'
      ) marm_out
;
