USE [EDW]
GO

/****** Object:  StoredProcedure [dbo].[spDlyISFactISSubClaimInsert]    Script Date: 28/3/2025 7:00:39 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Description : CRQ000000075476
--				insertion script for WBISIS FactISSubClaim to store Rider's Claim Details
--				FactISSubClaimHist will be used from function dbo.fnGetFactISSubClaim which is called from Tableau Data Source called "IS Claim DataSource"
--				BusinessKey: ClaimSeqID+CoveredItemID
--				DateKey: ClaimReportedDate
-- Create Date : 9-Sep-2019  
-- Written by  : May
-- Amendment History :  -- Mod Date		By			Description  
-- ------		----------	-------------- 

CREATE PROCEDURE [dbo].[spDlyISFactISSubClaimInsert] (@LastRunDate As Varchar(30) = Null)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @vLastRunDate AS varchar(11);
	SET @vLastRunDate = CONVERT(varchar(11), CONVERT(datetime, @LastRunDate), 106); 

	TRUNCATE TABLE stg.FactISSubClaim;

	INSERT INTO stg.FactISSubClaim
    SELECT
		clm.ClaimSeqId		
		,clm.ClaimEventNo
		,clm.ClaimNo
		,ISSubClaimNo = sc.ClaimNo
		,clm.ClaimReportedDate
		,ClaimReportedDateKey = ISNULL(CAST(FORMAT(clm.ClaimReportedDate, 'yyyyMMdd') AS NUMERIC), -1)
		,PolicySeqId = ISNULL(pci.PolicySeqId, -1)
		,sc.PolicyID
		,sc.PolicyNo
		,PolicyCoveredItemSeqId = ISNULL(pci.PolicyCoveredItemSeqId,-1)
		,CoveredItemId = sc.RiderID
		,SubClaimDateClosed = sc.DateClosed
		,SubClaimDateClosedKey = ISNULL(CAST(FORMAT(sc.DateClosed, 'yyyyMMdd') AS NUMERIC), -1)
		,SubClaimStatusSeqId = ISNULL(cstt.RefClaimStatusSeqId,-1)
		,SubClaimStatusCode = sc.Status
		,NoOfDays = ISNULL(sca.NoOfDays,0)
		,DailyBenefit = ISNULL(sca.DailyBenefit,0)
		,GetWellBenefit = ISNULL(sca.GetWellBenefit,0)
		,TotalPayOut = ISNULL(sca.Total_Payout,0)
		,RecInsertDate = GetDate()
		,RecUpdateDate = GetDate()	
	FROM RSRpt.WBCS.ISSubClaims sc
	INNER JOIN DimClaim clm on REPLACE (REPLACE (sc.Claimno ,'H',''),'C','') = clm.ClaimNo AND Appsource = 'WBISIS'	
	LEFT OUTER JOIN RSRpt.WBCS.ISSubClaimsAssess sca on sca.ClaimNo = sc.ClaimNo
	LEFT OUTER JOIN DimClaimStatusMapping cstt on cstt.ClaimStatusCode = sc.Status AND cstt.Appsource = 'WBISIS'	
	LEFT OUTER JOIN DimPolicyCoveredItem pci on pci.PolicyID = sc.PolicyID AND pci.CoveredItemID = sc.RiderID AND pci.AppSource = 'WBISIS'
	WHERE @vLastRunDate IS NULL 
	OR  (
			@vLastRunDate IS NOT NULL 
			AND 
			(
				sc.RSTimestamp >= @vLastRunDate
				OR sca.RSTimestamp >= @vLastRunDate
				OR clm.RecUpdateDate >= @vLastRunDate
			)
		) 
	 
END

GO


