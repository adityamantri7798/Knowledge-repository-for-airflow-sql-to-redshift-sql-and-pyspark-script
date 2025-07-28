BEGIN;

SET TIMEZONE = 'Singapore';

CREATE TABLE #v_rundate AS
SELECT nvl(CAST(DATE_TRUNC('day',DATEADD (day,-1,src_record_eff_from_date)) AS TIMESTAMP),CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS v_vLastRunDate
FROM (SELECT MAX(src_record_eff_from_date) AS src_record_eff_from_date
      FROM el_eds_def_stg.ctrl_audit 
	WHERE
    tgt_table_name='factissubclaim' and 
	tgt_source_app_code='ISIS' );


  
INSERT INTO el_eds_def.factissubclaim
		(
			source_app_code,
			source_data_set,
			dml_ind,
			record_created_date,
			record_updated_date,
			record_created_by,
			record_updated_by,
			record_eff_from_date,
			record_eff_to_date,
			active_record_ind,
			is_sub_claim_uuid,
		    business_key
		) 
		select
			 'MANUAL' as source_app_code,
			'MANUAL' as source_data_set,
			'I' AS dml_ind,
			GETDATE() AS record_created_date,
			GETDATE() AS record_updated_date,
			'EDS' AS record_created_by,
			'EDS' AS record_updated_by,
			CAST('1900-01-01 00:00:00.000000' AS timestamp) AS record_eff_from_date,
			CAST('9999-12-31 00:00:00.000000' AS timestamp) AS record_eff_to_date,
			'Y' AS active_record_ind,
			'-1' AS is_sub_claim_uuid,
			('MANUAL' || '~' || -1) AS business_key
		WHERE (
			SELECT COUNT(1) FROM el_eds_def.factissubclaim WHERE is_sub_claim_uuid = '-1'
			) = 0;
  
  
create table #tb_issubclaims_hist as
select
dml_ind,
record_eff_from_date,
active_record_ind,
claimno,
policyno,
policyid,
riderid,
dateclosed,
status
from ( select 
dml_ind,
record_eff_from_date,
active_record_ind,
claimno,
policyno,
policyid,
riderid,
dateclosed,
status,
row_number()over(partition by business_key order by coalesce(change_seq,-1) desc, record_eff_from_date desc, record_eff_to_date desc) rnk from tl_wbcs_def.tb_issubclaims_hist) 
where rnk=1 ;
  
create table #DimClaim as 
select 
source_app_code,
dml_ind,
record_updated_date,
record_eff_from_date,
record_eff_to_date,
Claim_uuid,
Claim_Event_No,
Claim_No,
Claim_Reported_Date
 from 
( select 
source_app_code,
dml_ind,
record_updated_date,
record_eff_from_date,
record_eff_to_date,
Claim_uuid,
Claim_Event_No,
Claim_No,
Claim_Reported_Date,
row_number()over(partition by business_key order by record_eff_from_date desc, record_eff_to_date desc) rnk 
from el_eds_def.Dimclaim
WHERE source_app_code = 'ISIS') where rnk=1;

create table #tb_issubclaimsassess_hist as 
select 
dml_ind,
record_eff_from_date,
noofdays,
dailybenefit,
getwellbenefit,
total_payout,
claimno
from tl_wbcs_def.tb_issubclaimsassess_hist
WHERE active_record_ind = 'Y' ;

create table #dimclaimstatusmapping as 
select
source_app_code,
dml_ind,
record_eff_from_date, 
Ref_Claim_Status_id,
Claim_Status_Code
from el_eds_def.dimclaimstatusmapping
WHERE active_record_ind = 'Y' and source_app_code = 'ISIS';

create table #dimpolicycovereditem as 
select
source_app_code,
dml_ind,
record_eff_from_date, 
Policy_uuid,
Policy_Covered_Item_uuid,
Policy_ID,
Covered_Item_ID
from el_eds_def.dimpolicycovereditem
WHERE active_record_ind = 'Y' and source_app_code = 'ISIS';


CREATE TABLE #PKPrimary AS 
SELECT DISTINCT claimno,riderid
FROM
(
		SELECT 
			sc.riderid,sc.claimno
		FROM 
		#tb_issubclaims_hist sc 
		WHERE sc.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) 
	UNION  
		Select 
			sc.riderid,sc.claimno
		FROM 
		#tb_issubclaims_hist sc 
		 inner join #tb_issubclaimsassess_hist sca on sca.ClaimNo = sc.ClaimNo
		WHERE sca.record_eff_from_date >=(SELECT v_vLastRunDate FROM #v_rundate) );
			
CREATE TABLE #tempstgfactissubclaim as
		SELECT 
		     'ISIS' as source_app_code
			,'ISIS' as source_data_set
			,sc.dml_ind as dml_ind
			,sc.record_eff_from_date as record_eff_from_date
		    ,sha2('ISIS' ||'~'||clm.Claim_No||'~'||sc.RiderID, 256) as is_sub_claim_uuid
		,('ISIS' ||'~'||clm.Claim_No||'~'||sc.RiderID) as business_key
			,clm.Claim_uuid as claim_uuid
			,clm.Claim_Event_No as Claim_Event_No
            ,clm.Claim_No as Claim_No
			,sc.ClaimNo as IS_Sub_Claim_No
			,clm.Claim_Reported_Date as Claim_Reported_Date
			,ISNULL(CAST(to_char(clm.Claim_Reported_Date, 'yyyyMMdd') AS NUMERIC), -1) as Claim_Reported_Date_Key
			,ISNULL(pci.Policy_uuid, '-1') as Policy_uuid
			,sc.PolicyID as policy_ID
		    ,sc.PolicyNo as Policy_No
			,ISNULL(pci.Policy_Covered_Item_uuid,'-1') as Policy_Covered_Item_uuid
			,sc.RiderID as Covered_Item_Id
			,sc.DateClosed as Sub_Claim_Date_Closed
			,ISNULL(CAST(to_char(sc.DateClosed, 'yyyyMMdd') AS NUMERIC), -1) as Sub_Claim_Date_Closed_Key
			,ISNULL(cstt.Ref_Claim_Status_id,-1) as Sub_Claim_Status_id
			,sc.Status as Sub_Claim_Status_Code
			,ISNULL(sca.NoOfDays,0) as No_Of_Days
			,ISNULL(sca.DailyBenefit,0) as Daily_Benefit
			,ISNULL(sca.GetWellBenefit,0) as Get_Well_Benefit
			,ISNULL(sca.Total_Payout,0) as Total_PayOut
		 FROM #PKPrimary pk INNER JOIN  #tb_issubclaims_hist sc on pk.claimno =sc.claimno and pk.riderid=sc.Riderid
	INNER JOIN #DimClaim clm on REPLACE (REPLACE (sc.Claimno ,'H',''),'C','') = clm.Claim_No 	
	LEFT OUTER JOIN #tb_issubclaimsassess_hist sca on sca.ClaimNo = sc.ClaimNo
	LEFT OUTER JOIN #DimClaimStatusMapping cstt on cstt.Claim_Status_Code = sc.Status 	
	LEFT OUTER JOIN #dimpolicycovereditem pci on pci.Policy_ID = sc.PolicyID AND pci.Covered_Item_ID = sc.RiderID;
	

				   
				   
create table #hashStgfactissubclaim as
SELECT  source_app_code
		,source_data_set
		,dml_ind
		,record_eff_from_date 
		,is_sub_claim_uuid
		,business_key
		,claim_uuid	
		,Claim_Event_No
		,Claim_No
		,is_sub_claim_no
		,Claim_Reported_Date
		,Claim_Reported_Date_Key
		,Policy_uuid
		,Policy_Id
		,Policy_No
		,Policy_Covered_Item_uuid
		,Covered_Item_Id
		,Sub_Claim_Date_Closed
		,Sub_Claim_Date_Closed_Key
		,Sub_Claim_Status_id
		,Sub_Claim_Status_Code
		,No_Of_Days
		,Daily_Benefit
		,Get_Well_Benefit
		,Total_PayOut
		,sha2(
		coalesce(cast(source_app_code as varchar),cast('null' as varchar))+
		coalesce(cast(source_data_set as varchar),cast('null' as varchar))+
		coalesce(cast(is_sub_claim_uuid as varchar),cast('null' as varchar))+
		coalesce(cast(business_key as varchar),cast('null' as varchar))+
		coalesce(cast(Claim_uuid as varchar),cast('null' as varchar))+
coalesce(cast(Claim_Event_No as varchar),cast('null' as varchar))+
coalesce(cast(Claim_No as varchar),cast('null' as varchar))+
coalesce(cast(is_sub_claim_no as varchar),cast('null' as varchar))+
coalesce(cast(Claim_Reported_Date as varchar),cast('null' as varchar))+
coalesce(cast(Claim_Reported_Date_Key as varchar),cast('null' as varchar))+
coalesce(cast(Policy_uuid as varchar),cast('null' as varchar))+

coalesce(cast(Policy_Id as varchar),cast('null' as varchar))+
coalesce(cast(Policy_No as varchar),cast('null' as varchar))+
coalesce(cast(Policy_Covered_Item_uuid as varchar),cast('null' as varchar))+
coalesce(cast(Covered_Item_Id as varchar),cast('null' as varchar))+
coalesce(cast(Sub_Claim_Date_Closed as varchar),cast('null' as varchar))+
coalesce(cast(Sub_Claim_Date_Closed_Key as varchar),cast('null' as varchar))+
coalesce(cast(Sub_Claim_Status_id as varchar),cast('null' as varchar))+
coalesce(cast(Sub_Claim_Status_Code as varchar),cast('null' as varchar))+
coalesce(cast(No_Of_Days as varchar),cast('null' as varchar))+
coalesce(cast(Daily_Benefit as varchar),cast('null' as varchar))+
coalesce(cast(Get_Well_Benefit as varchar),cast('null' as varchar))+
coalesce(cast(Total_PayOut as varchar),cast('null' as varchar)),256 )
 as checksum from #tempstgfactissubclaim;

create table #stgfactissubclaim AS
select * from (select a.* , case when a.checksum <> coalesce(b.checksum,'1') then 1 when a.dml_ind='D' then 1 else 0 end as changed_rec_check from #hashStgfactissubclaim a left outer join el_eds_def.factissubclaim b on a.business_key = b.business_key and b.source_app_code='ISIS'  and b.active_record_ind='Y' ) where changed_rec_check =1;



DELETE FROM el_eds_def_stg.stgfactissubclaim  where source_app_code='ISIS';

INSERT INTO el_eds_def_stg.stgfactissubclaim 
    (    
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
		,Claim_Event_No
		,Claim_No
		,is_sub_claim_no
		,Claim_Reported_Date
		,Claim_Reported_Date_Key
		,Policy_uuid
		,Policy_Id
		,Policy_No
		,Policy_Covered_Item_uuid
		,Covered_Item_Id
		,Sub_Claim_Date_Closed
		,Sub_Claim_Date_Closed_Key
		,Sub_Claim_Status_id
		,Sub_Claim_Status_Code
		,No_Of_Days
		,Daily_Benefit
		,Get_Well_Benefit
		,Total_PayOut
			)
			select
			      source_app_code
		,source_data_set
		,dml_ind
		,getdate() as record_created_date
		,getdate() as record_updated_date
		,'EDS' AS record_created_by
		,'EDS' as record_updated_by
		,record_eff_from_date
		,cast('9999-12-31 00:00:00.000000' as timestamp) as record_eff_to_date 
		,'Y' as active_record_ind
		,checksum
		,is_sub_claim_uuid
		,business_key
		,claim_uuid	
		,Claim_Event_No
		,Claim_No
		,is_sub_claim_no
		,Claim_Reported_Date
		,Claim_Reported_Date_Key
		,Policy_uuid
		,Policy_Id
		,Policy_No
		,Policy_Covered_Item_uuid
		,Covered_Item_Id
		,Sub_Claim_Date_Closed
		,Sub_Claim_Date_Closed_Key
		,Sub_Claim_Status_id
		,Sub_Claim_Status_Code
		,No_Of_Days
		,Daily_Benefit
		,Get_Well_Benefit
		,Total_PayOut
	from #stgfactissubclaim;
	
	
	
DROP TABLE IF EXISTS #tb_issubclaims_hist;
DROP TABLE IF EXISTS #DimClaim;
DROP TABLE IF EXISTS #tb_issubclaimsassess_hist;
DROP TABLE IF EXISTS #DimClaimStatusMapping;
DROP TABLE IF EXISTS #tempstgfactissubclaim;
DROP TABLE IF EXISTS #hashStgfactissubclaim;
DROP TABLE IF EXISTS #stgfactissubclaim;
DROP TABLE IF EXISTS #v_rundate;
DROP TABLE IF EXISTS #PKPrimary;


END;
