BEGIN;

SET  TIMEZONE = 'Singapore';

-- Creating one temptable by union both finaltemp and final table to check whether businesskey is already present or not in final table
CREATE TABLE #tempstgfactissubclaim AS 
SELECT source_app_code ,source_data_set, dml_ind,  
case when latest_record_created_date is null then record_created_date else latest_record_created_date END AS record_created_date, 
record_updated_date, 
record_created_by, 
record_updated_by, 
record_eff_from_date, 
CASE WHEN dml_ind <> 'D' AND is_sub_claim_uuid IS NULL 
THEN date_trunc(
  'second', to_timestamp('9999-12-31', 'yyyy-MM-dd')
) 
WHEN dml_ind <> 'D' AND is_sub_claim_uuid IS NOT NULL AND rnk = 1 
THEN date_trunc(
  'second', 
  to_timestamp('9999-12-31', 'yyyy-MM-dd')
) 
WHEN dml_ind = 'D' then record_eff_from_date ELSE latest_record_eff_from_date END AS record_eff_to_date, 
CASE WHEN dml_ind <> 'D' 
AND is_sub_claim_uuid IS NULL THEN 'Y' WHEN dml_ind <> 'D'
AND is_sub_claim_uuid IS NOT NULL 
AND rnk = 1 THEN 'Y' WHEN dml_ind = 'D' then 'N' ELSE 'N' END AS active_record_ind
,checksum
,is_sub_claim_uuid
,business_key
,claim_uuid	
		,claim_event_no
		,claim_no
		,is_sub_claim_no
		,claim_reported_date
		,claim_reported_date_key
		,policy_uuid
		,policy_id
		,policy_no
		,policy_covered_item_uuid
		,covered_item_id
		,sub_claim_date_closed
		,sub_claim_date_closed_key
		,sub_claim_status_id
		,sub_claim_status_code
		,no_of_days
		,daily_benefit
		,get_well_benefit
		,total_payout
		,table_type
    ,latest_record_eff_from_date
    ,rnk
FROM 
   (  
    SELECT 
        temp.*, 
        ROW_NUMBER() OVER (
          PARTITION BY temp.is_sub_claim_uuid 
          ORDER BY 
            temp.record_eff_from_date DESC
        ) AS rnk, 
        LAG(record_eff_from_date) OVER (
          PARTITION BY temp.is_sub_claim_uuid 
          ORDER BY 
            temp.record_eff_from_date DESC
        ) AS latest_record_eff_from_date,  
        LEAD(record_created_date) OVER (
          PARTITION BY business_key 
          ORDER BY 
            record_eff_from_date DESC
        ) AS latest_record_created_date 
    FROM 
      (
	   SELECT 
			source_app_code 
			,source_data_set 
			,dml_ind 
			,record_created_date
			,record_updated_date
			,record_created_by 
			,record_updated_by 
			,record_eff_from_date 
			,record_eff_to_date 
			,active_record_ind
			,checksum			
		    ,is_sub_claim_uuid
            ,business_key
            ,claim_uuid	
		,claim_event_no
		,claim_no
		,is_sub_claim_no
		,claim_reported_date
		,claim_reported_date_key
		,policy_uuid
		,policy_id
		,policy_no
		,policy_covered_item_uuid
		,covered_item_id
		,sub_claim_date_closed
		,sub_claim_date_closed_key
		,sub_claim_status_id
		,sub_claim_status_code
		,no_of_days
		,daily_benefit
		,get_well_benefit
		,total_payout
         
			,'TEMP' as table_type
		 FROM 
          el_eds_def_stg.stgfactissubclaim where source_app_code='ISIS'
        UNION 
           (
        SELECT
			 source_app_code 
			,source_data_set 
			,dml_ind 
			,record_created_date
			,record_updated_date
			,record_created_by 
			,record_updated_by 
			,record_eff_from_date 
			,record_eff_to_date 
			,active_record_ind
			,checksum			
		    ,is_sub_claim_uuid
		   ,business_key
		,claim_uuid	
		,claim_event_no
		,claim_no
		,is_sub_claim_no
		,claim_reported_date
		,claim_reported_date_key
		,policy_uuid
		,policy_id
		,policy_no
		,policy_covered_item_uuid
		,covered_item_id
		,sub_claim_date_closed
		,sub_claim_date_closed_key
		,sub_claim_status_id
		,sub_claim_status_code
		,no_of_days
		,daily_benefit
		,get_well_benefit
		,total_payout
            ,'HIST' as table_type

FROM
    el_eds_def.factissubclaim b	
                where 
                  b.business_key in (
                SELECT 
                  business_key 
                FROM 
                  el_eds_def_stg.stgfactissubclaim a 
                where 
                  a.record_eff_from_date <> b.record_eff_from_date and a.source_app_code = 'ISIS'
            ) 
            AND b.active_record_ind = 'Y' 
            and b.source_app_code = 'ISIS'
        )
    )temp
)temp;




--Merging the temp table with final table 
MERGE INTO el_eds_def.factissubclaim  
USING #tempstgfactissubclaim temp ON el_eds_def.factissubclaim.business_key = temp.business_key 
AND el_eds_def.factissubclaim.record_eff_from_date= temp.record_eff_from_date and el_eds_def.factissubclaim.source_app_code ='ISIS' 
WHEN MATCHED
THEN UPDATE	  
SET
record_updated_date= temp.record_updated_date
,record_eff_to_date = case WHEN el_eds_def.factissubclaim.active_record_ind = 'Y' AND temp.rnk != 1 then temp.latest_record_eff_from_date else el_eds_def.factissubclaim.record_eff_to_date END
,active_record_ind = case WHEN el_eds_def.factissubclaim.active_record_ind = 'Y' AND temp.rnk != 1 then temp.active_record_ind else el_eds_def.factissubclaim.active_record_ind END
,checksum=temp.checksum
,claim_uuid=temp.claim_uuid
,claim_event_no=temp.claim_event_no
,claim_no=temp.claim_no
,is_sub_claim_no=temp.is_sub_claim_no
,claim_reported_date=temp.claim_reported_date
,claim_reported_date_key=temp.claim_reported_date_key
,policy_uuid=temp.policy_uuid
,policy_id=temp.policy_id
,policy_no=temp.policy_no
,policy_covered_item_uuid=temp.policy_covered_item_uuid
,covered_item_id=temp.covered_item_id
,sub_claim_date_closed=temp.sub_claim_date_closed
,sub_claim_date_closed_key=temp.sub_claim_date_closed_key
,sub_claim_status_id=temp.sub_claim_status_id
,sub_claim_status_code=temp.sub_claim_status_code
,no_of_days=temp.no_of_days
,daily_benefit=temp.daily_benefit
,get_well_benefit=temp.get_well_benefit
,total_payout=temp.total_payout


WHEN NOT MATCHED
THEN insert(
 source_app_code
,source_data_set
,dml_ind
,record_created_date
,record_updated_date
,record_created_by
,record_updated_by
,record_eff_from_date
,record_eff_to_date
,active_record_ind
,checksum
,is_sub_claim_uuid
,business_key
,claim_uuid	
		,claim_event_no
		,claim_no
		,is_sub_claim_no
		,claim_reported_date
		,claim_reported_date_key
		,policy_uuid
		,policy_id
		,policy_no
		,policy_covered_item_uuid
		,covered_item_id
		,sub_claim_date_closed
		,sub_claim_date_closed_key
		,sub_claim_status_id
		,sub_claim_status_code
		,no_of_days
		,daily_benefit
		,get_well_benefit
		,total_payout
	)
values(
temp.source_app_code
,temp.source_data_set
,temp.dml_ind
,temp.record_created_date
,temp.record_updated_date
,temp.record_created_by
,temp.record_updated_by
,temp.record_eff_from_date
,temp.record_eff_to_date
,temp.active_record_ind
,temp.checksum
,temp.is_sub_claim_uuid
,temp.business_key
,temp.claim_uuid
,temp.claim_event_no
,temp.claim_no
,temp.is_sub_claim_no
,temp.claim_reported_date
,temp.claim_reported_date_key
,temp.policy_uuid
,temp.policy_id
,temp.policy_no
,temp.policy_covered_item_uuid
,temp.covered_item_id
,temp.sub_claim_date_closed
,temp.sub_claim_date_closed_key
,temp.sub_claim_status_id
,temp.sub_claim_status_code
,temp.no_of_days
,temp.daily_benefit
,temp.get_well_benefit
,temp.total_payout

);

	
DROP TABLE IF EXISTS #tempstgfactissubclaim;

END;