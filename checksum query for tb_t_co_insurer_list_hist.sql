SELECT target.business_key, driver.checksum d_check, target.checksum AS t_check from (

    SELECT business_key,

        md5(

            concat(

                coalesce(cast(cast(cast(co_list_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(policy_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
                coalesce(cast(cast(co_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),

                coalesce(cast(cast(ref_policy_no AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(leader_flag as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(insert_time as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(party_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(si_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(commision_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(other_fee AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(co_fee_rate as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(update_time AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(field01 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field02 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field03 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field04 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field05 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field06 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field07 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field08 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field09 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field10 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field11 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field12 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(field13 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field14 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field15 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(field16 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field17 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field18 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field19 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field20 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary))
				
				
            )

        ) AS checksum

    FROM tl_ebgi_def.tb_t_co_insurer_list_hist WHERE active_record_ind IN ('Y', 'y')

) target

INNER JOIN (

    SELECT business_key,

        md5(

           concat(

               coalesce(cast(cast(cast(co_list_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(policy_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
                coalesce(cast(cast(co_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),

                coalesce(cast(cast(ref_policy_no AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(leader_flag as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(insert_time as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(party_id as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(si_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(commision_share AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(other_fee AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(co_fee_rate as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(update_time AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(field01 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field02 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field03 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field04 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field05 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field06 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field07 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field08 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field09 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field10 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field11 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field12 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(cast(field13 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field14 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field15 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				
				coalesce(cast(cast(field16 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field17 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field18 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(field19 AS varchar) AS varbinary), cast('NULL' AS varbinary)),
				coalesce(cast(cast(cast(field20 as varchar) AS varchar) AS varbinary), cast('NULL' AS varbinary))
 
         )

        ) AS checksum,dml_ind,row_number() OVER (PARTITION BY business_key ORDER BY cdc_source_commit_date DESC) AS rnk

    FROM rl_ebgi_gs.tb_t_co_insurer_list_de d
	
  where  dml_ind not in ('D', 'd')
    ) driver 
    on target.business_key=driver.business_key 
where driver.rnk=1 and target.checksum<>driver.checksum;