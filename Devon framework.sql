--build date
--build source
--build metric fact


--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw	VIEW
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_DateDim	BASE TABLE
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_Detail_vw	VIEW
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_Diagnosis_vw	VIEW
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_Encounter_vw	VIEW
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_MetricDim	BASE TABLE
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_MetricFact	BASE TABLE
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_OutcomeFact	BASE TABLE
--RADB	dbo	CRD_ERAS_YNHOBGYN_31893_ProcessMetrics	BASE TABLE

SELECT * 
FROM RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact
WHERE PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact GROUP BY PAT_ENC_CSN_ID HAVING COUNT(*)>1)


SELECT *
FROM CRD_ERAS_YNHOBGYN_31893_ProcessMetrics



SELECT * FROM 
RADB.dbo.CRD_ERAS_Global_TimeWindows_vw

sp_helptext CRD_ERAS_Global_TimeWindows_vw

SELECT * 
FROM RADB.dbo.CRD_ERAS_Global_TimeWindows
WHERE LOG_ID=439155

SELECT * 
FROM clarity.dbo.V_LOG_TIMING_EVENTS 
WHERE LOG_ID=439155


sp_helptext V_LOG_TIMING_EVENTS 

SELECT * FROM 
radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics
WHERE log_id IN (

SELECT log_id
FROM CRD_ERAS_YNHOBGYN_31893_ProcessMetrics
GROUP BY log_id HAVING COUNT(*)>1
)
ORDER BY log_id

SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%31893%'

SELECT * FROM CRD_ERAS_YNHOBGYN_31893_Detail_vw

SELECT * 
FROM CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw

SELECT * FROM dbo.CRD_ERAS_YNHOBGYN_31893_MetricFact AS ceymf

SELECT * FROM dbo.CRD_ERAS_YNHOBGYN_31893_MetricDim AS ceymd

SELECT * FROM CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw

sp_helptext CRD_ERAS_YNHOBGYN_31893_Detail_vw



SELECT * FROM 	radb.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact
SELECT * FROM   radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics	
--primary key?
SELECT * FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact
WHERE PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact GROUP BY PAT_ENC_CSN_ID HAVING COUNT(*)>1)

SELECT * FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics 
WHERE PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics GROUP BY PAT_ENC_CSN_ID HAVING COUNT(*)>1)

SELECT * FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics 
WHERE LOG_ID IN (SELECT LOG_ID fROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics GROUP BY log_id HAVING COUNT(*)>1)



SELECT * FROM CRD_ERAS_YNHOBGYN_31893_Diagnosis_vw	
SELECT * FROM CRD_ERAS_YNHOBGYN_31893_Diagnosis_vw	

sp_helptext CRD_ERAS_YNHOBGYN_31893_Diagnosis_vw	





USE [RADB]
GO
/****** Object:  StoredProcedure [dbo].[CRD_ERAS_YNHOBGYN_31893_Build_Source]    Script Date: 7/11/2016 5:22:50 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[CRD_ERAS_YNHOBGYN_31893_Build_Source]
AS

/*****************************************************************************************************************
Title: GYN data by CPT code
Author: Busheydm
Created: 4/7/2016
Purpose: Clinical Redesign

Description:
To be used for the Clinical Redesign project #31893

Updates:
	04/19/2016 DDB: Fixed CPT code look up.  Data has extra linefeeds causing join issues

*****************************************************************************************************************/

/*****************************************************************************************************************

Declare Variables

*****************************************************************************************************************/

DECLARE @StartDate DATETIME = '4/1/2015'
DECLARE @EndDate DATETIME = GETDATE()




/*****************************************************************************************************************
GET list of base encounters
*****************************************************************************************************************/

IF OBJECT_ID(N'tempdb..#Encounters') IS NOT NULL
BEGIN
DROP TABLE #Encounters
End




	SELECT
		ENC.PAT_ENC_CSN_ID
	   ,EncLink.OR_LINK_CSN
	   ,Or_Log.LOG_ID
	   ,CPT.REAL_CPT_CODE
	   ,OR_Proc.OR_PROC_ID
	   ,Or_Log.SURGERY_DATE
	   ,ENC.PAT_ID
	   ,Anes.AN_52_ENC_CSN_ID 'Anes_Enc_Csn_ID'
	INTO
		#Encounters
	FROM
		Clarity.dbo.OR_LOG Or_Log
	LEFT JOIN Clarity.dbo.OR_LOG_ALL_PROC Lproc ON Or_Log.LOG_ID = Lproc.LOG_ID
	LEFT JOIN Clarity.dbo.OR_PROC OR_Proc ON ( Lproc.OR_PROC_ID = OR_Proc.OR_PROC_ID )
	LEFT JOIN Clarity.dbo.PAT_OR_ADM_LINK EncLink ON ( EncLink.LOG_ID = Or_Log.LOG_ID )
	LEFT JOIN Clarity.dbo.PAT_ENC_HSP ENC ON ( ENC.PAT_ENC_CSN_ID = EncLink.OR_LINK_CSN )
	LEFT JOIN Clarity.dbo.OR_PROC_CPT_ID CPT ON OR_Proc.OR_PROC_ID = CPT.OR_PROC_ID
	LEFT JOIN Clarity.dbo.F_AN_RECORD_SUMMARY Anes ON Anes.LOG_ID = Or_Log.LOG_ID
	/*Cpt Code Reference Table*/
	JOIN RADB.dbo.CRD_GYN_CPTCodeDim CPT_Ref ON CPT.REAL_CPT_CODE = CPT_Ref.CPTCode

	WHERE
		--CPT.REAL_CPT_CODE IN ( '58150' )
		--AND 
		--ENC.HOSP_DISCH_TIME BETWEEN '4/1/2015' AND '4/1/2016'
		
		or_log.SURGERY_DATE BETWEEN @StartDate AND @EndDate
		AND Or_Log.STATUS_C <> 6





/*****************************************************************************************************************

Get Primary Diagnosis

*****************************************************************************************************************/


IF OBJECT_ID(N'tempdb..#Diag') IS NOT NULL
BEGIN
DROP TABLE #Diag
End

	SELECT DxList.HSP_ACCOUNT_ID,
		   ENC.PAT_ENC_CSN_ID,
		   DxList.DX_ID ,
		   EDG.REF_BILL_CODE,
		   EDG.DX_NAME
	INTO #Diag
	FROM clarity.dbo.HSP_ACCT_DX_LIST DxList
		JOIN Clarity.dbo.CLARITY_EDG EDG ON EDG.DX_ID = DxList.DX_ID
		LEFT JOIN Clarity.dbo.ZC_DX_POA POA ON DxList.FINAL_DX_POA_C = poa.DX_POA_C
        JOIN  (
				SELECT HSP_ACCOUNT_ID, PAT_ENC_CSN_ID
				FROM Clarity.dbo.PAT_ENC_HSP
				WHERE PAT_ENC_CSN_ID IN (
											SELECT PAT_ENC_CSN_ID 
											FROM #Encounters 
											GROUP BY PAT_ENC_CSN_ID
										)
			 ) Enc ON Enc.HSP_ACCOUNT_ID = DxList.HSP_ACCOUNT_ID

	WHERE 
	DxList.DX_ID IS NOT NULL
	AND DxList.LINE = 1 



/*****************************************************************************************************************/


/*****************************************************************************************************************

/*This Snipet will return the initial data dump*/
/*Source Data to be used as the basis for most of the pull*/

*****************************************************************************************************************/

/*10101 - York
  10201 - SRC
  10117 - Temple

*/


IF OBJECT_ID(N'tempdb..#Source') IS NOT NULL
BEGIN
DROP TABLE #Source
End

SELECT
enc.PAT_ENC_CSN_ID 
,Anes_Enc_Csn_ID
,e.log_ID
,Dep.Loc_Name
,dep.Loc_ID
--,EncLink.OR_LINK_CSN 
--,Or_Log.LOG_ID
,pat.PAT_MRN_ID
,pat.PAT_ID
,ser.PROV_ID
,SER.PROV_NAME
,Or_Log.SURGERY_DATE
,CPT_Ref.ProcedureType
,CPT.REAL_CPT_CODE
,enc.HOSP_ADMSN_TIME
,enc.HOSP_DISCH_TIME
,DATEDIFF(HOUR,ENC.HOSP_ADMSN_TIME,enc.HOSP_DISCH_TIME) 'LOS_Hours'
,DATEDIFF(Day,ENC.HOSP_ADMSN_TIME,enc.HOSP_DISCH_TIME) 'LOS_Days'
,dep.Department_Name 'Discharge_Dep'
,dep.Department_ID 'Discharge_DepID'
,AT.NAME 'AdmitType'
,di.REF_BILL_CODE 'ICD_Code'
,di.DX_NAME 'ICD_Name'
,Lproc.PROC_DISPLAY_NAME


INTO  #Source
FROM
#Encounters E
JOIN Clarity.dbo.OR_LOG Or_Log ON E.LOG_ID = Or_Log.LOG_ID 
/*Procedure Information*/
LEFT JOIN Clarity.dbo.OR_LOG_ALL_PROC Lproc ON Or_log.LOG_ID = Lproc.LOG_ID AND Lproc.OR_PROC_ID = E.OR_PROC_ID
LEFT JOIN Clarity.dbo.OR_PROC OR_Proc ON ( Lproc.OR_PROC_ID = OR_Proc.OR_PROC_ID )

/*Primary Surgeon*/
LEFT JOIN Clarity.dbo.OR_LOG_ALL_SURG Log_Surg ON ( OR_LOG.LOG_ID = Log_Surg.LOG_ID )
											   AND Log_Surg.ROLE_C = 1
											   AND Log_Surg.PANEL = 1 --primary surgeon only
LEFT JOIN clarity.dbo.CLARITY_SER SER ON SER.PROV_ID = Log_Surg.SURG_ID
/*Encounter Link + Information*/
LEFT JOIN Clarity.dbo.PAT_OR_ADM_LINK EncLink ON ( EncLink.LOG_ID = Or_Log.LOG_ID )
LEFT JOIN Clarity.dbo.PAT_ENC_HSP ENC ON ( ENC.PAT_ENC_CSN_ID = EncLink.OR_LINK_CSN )
LEFT JOIN Clarity.dbo.HSP_ACCOUNT HSP ON ENC.HSP_ACCOUNT_ID =HSP.HSP_ACCOUNT_ID
LEFT JOIN Clarity.dbo.HSP_ACCOUNT_3 HSP3 ON HSP.HSP_ACCOUNT_ID = HSP3.HSP_ACCOUNT_ID
LEFT JOIN Clarity.dbo.ZC_HOSP_ADMSN_TYPE AT ON HSP3.ADMIT_TYPE_EPT_C = at.HOSP_ADMSN_TYPE_C
/*CPT Information*/
LEFT JOIN Clarity.dbo.OR_PROC_CPT_ID CPT ON OR_Proc.OR_PROC_ID = CPT.OR_PROC_ID
Left JOIN RADB.dbo.CRD_GYN_CPTCodeDim CPT_Ref ON CPT.REAL_CPT_CODE = CPT_Ref.CPTCode
/*Patient Information*/
LEFT JOIN clarity.dbo.PATIENT Pat ON pat.PAT_ID = ENC.PAT_ID
/*Discharge Unit*/
LEFT JOIN RADB.dbo.vw_LocHierarchy_Department Dep ON dep.Department_ID = enc.DEPARTMENT_ID
LEFT JOIN #Diag Di ON di.PAT_ENC_CSN_ID = E.PAT_ENC_CSN_ID
WHERE dep.Loc_ID IN (10101, 10201, 10117) /*YSC, SRC, Temple*/
GROUP BY 
enc.PAT_ENC_CSN_ID 
,Anes_Enc_Csn_ID
,e.log_ID
,Dep.Loc_Name
,dep.Loc_ID
--,EncLink.OR_LINK_CSN 
--,Or_Log.LOG_ID
,CPT.REAL_CPT_CODE
,enc.HOSP_ADMSN_TIME
,enc.HOSP_DISCH_TIME
,pat.PAT_MRN_ID
,pat.PAT_ID
,ser.PROV_ID
,SER.PROV_NAME
,Or_Log.SURGERY_DATE
,CPT_Ref.ProcedureType
,DATEDIFF(HOUR,ENC.HOSP_ADMSN_TIME,enc.HOSP_DISCH_TIME) 
,DATEDIFF(Day,ENC.HOSP_ADMSN_TIME,enc.HOSP_DISCH_TIME) 
,dep.Department_Name 
,dep.Department_ID 
,AT.NAME
,di.REF_BILL_CODE 
,di.DX_NAME 
,Lproc.PROC_DISPLAY_NAME



--SELECT PAT_ENC_CSN_ID, SUM(1) FROM #Source GROUP BY PAT_ENC_CSN_ID HAVING sum(1) > 1

--SELECT * FROM #Source WHERE PAT_ENC_CSN_ID = '124821132'


/*
	/*There is one encoutner that comes up as a dup due to the display name, it is more than likely bad data based on the nature of it
		Need to revisit to possible build in a second pass to remove these types.

		For now I have not done anything as it does not really affect any of the data.
	*/

				SELECT PROC_DISPLAY_NAME,PROC_NAME,CPT.REAL_CPT_CODE,lproc.OR_PROC_ID,OR_Proc.OR_PROC_ID,* FROM clarity.dbo.OR_LOG_ALL_PROC lproc
				LEFT JOIN Clarity.dbo.OR_PROC OR_Proc ON ( Lproc.OR_PROC_ID = OR_Proc.OR_PROC_ID )
				LEFT JOIN Clarity.dbo.OR_PROC_CPT_ID CPT ON OR_Proc.OR_PROC_ID = CPT.OR_PROC_ID
				 JOIN RADB.dbo.CRD_GYN_CPTCodeDim CPT_Ref ON CPT.REAL_CPT_CODE = CPT_Ref.CPTCode
				WHERE LOG_ID = 479159


				SELECT * FROM Clarity.dbo.OR_PROC OR_Proc WHERE OR_Proc.OR_PROC_ID = '58573'

				--SELECT * FROM #Source WHERE PROC_DISPLAY_NAME LIKE '%ERAS%'
	*/

/*****************************************************************************************************************

Outcome Metrics

*****************************************************************************************************************/

	/*****************************************************************************************************************

	ED visit encounters

	-This can be used to flag an encounter as arriving through the ED
	-Also used to find if the patient reappeared in the ED following thier procedure

	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#EDRevisit') IS NOT NULL
	BEGIN
	DROP TABLE #EDRevisit
	End
	
	--SELECT * FROM #Source WHERE PAT_ENC_CSN_ID = '131764493'

		SELECT 
		x.PAT_ENC_CSN_ID
	   ,x.Idx_CSN
	   ,x.ED_ARRIVAL_TIME
	   ,x.SURGERY_DATE
	   ,x.EDvisit
	   ,x.ARRIVAL_LOCATION
	   ,x.ED_DISPOSITION
		INTO #EDRevisit 
		FROM (
			 SELECT 
			 Ed.PAT_ENC_CSN_ID
			 ,s.PAT_ENC_CSN_ID 'Idx_CSN'
			 ,ed.ED_ARRIVAL_TIME
			 ,s.SURGERY_DATE
			 ,DATEDIFF(DAY,s.SURGERY_DATE,ed.ED_ARRIVAL_TIME) 'EDvisit'
			 ,ED.ARRIVAL_LOCATION
			 ,ED.ED_DISPOSITION
		
			 From
			 [RADB].[dbo].[vw_YNHHS_ED_DATA] ED
			 JOIN (SELECT PAT_ENC_CSN_ID, PAT_ID, SURGERY_DATE FROM #Source GROUP BY PAT_ENC_CSN_ID, PAT_ID, SURGERY_DATE) S ON ED.PAT_ID = s.PAT_ID AND ed.ED_ARRIVAL_TIME > s.SURGERY_DATE
			 ) x
		JOIN #Source s2 ON  s2.PAT_ENC_CSN_ID = x.Idx_CSN 
						--AND x.Idx_CSN = x.PAT_ENC_CSN_ID 
						--AND s2.AdmitType <> 'Emergency'
		WHERE EDvisit <= 30
				and	0 = CASE WHEN (x.PAT_ENC_CSN_ID = x.Idx_CSN AND s2.AdmitType = 'Emergency') THEN 1 ELSE 0 END 
				
	 
	/*****************************************************************************************************************
	
	Reprocedure

	-Any time a procedure is performed a second time 
	
	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#RePo') IS NOT NULL
	BEGIN
	DROP TABLE #RePo
	End
	
	SELECT 
	Idx_CSN
	,ReProcedure_CSN
	,RePoOrder
	,ReProcedureCase
	INTO #RePo
	FROM
	 (
		SELECT
			idx.PAT_ENC_CSN_ID 'Idx_CSN'
			,RePo.PAT_ENC_CSN_ID 'ReProcedure_CSN'
			,ROW_NUMBER() OVER	(PARTITION BY idx.PAT_ENC_CSN_ID ORDER BY RePo.SURGERY_DATE) 'RePoOrder'
			, 1 AS 'ReProcedureCase'

		FROM
			#Source idx
		JOIN #encounters RePo ON idx.PAT_ID = RePo.PAT_ID
							 AND idx.REAL_CPT_CODE = RePo.REAL_CPT_CODE
							 AND idx.SURGERY_DATE < RePo.SURGERY_DATE
		) x
		WHERE x.RePoOrder = 1


	/*****************************************************************************************************************

	QVI Encounters

	-Get the QVI Encounters for the set of qaulifing cases

	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#QVI') IS NOT NULL
	BEGIN
	DROP TABLE #QVI
	End
	

	SELECT
		S.PAT_ENC_CSN_ID
	   ,MAX(QVI.Qvi_Value) 'Qvi_Value'
	INTO #QVI
	FROM
		RADB.dbo.QVI_Fact QVI
	JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.HSP_ACCOUNT_ID = QVI.HSP_ACCOUNT_ID
	JOIN #Source S ON S.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
	WHERE
		QVI_Hierarchy_Key = 42 /*42 - qvi_Infection_SurgicalSite*/
	GROUP BY
		S.PAT_ENC_CSN_ID


	/*****************************************************************************************************************
	
	Direct Cost Per Case

	-Using the Sourced table Craig K. brings over from Rediscovery
	
	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#Cost') IS NOT NULL
	BEGIN
	DROP TABLE #Cost
	End
	

	SELECT
		S.PAT_ENC_CSN_ID
	   ,MAX(cost.TotalDirectCost) 'TotalDirectCost'
	INTO #Cost
	FROM
		RADB.dbo.ReDiscovery_Costs cost
	JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.HSP_ACCOUNT_ID = cost.HSP_ACCOUNT_ID
	JOIN #Source S ON S.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
	GROUP BY
		S.PAT_ENC_CSN_ID


	/*****************************************************************************************************************
	
	Readmissions

	-Get the readmission cases
	
	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#ReAd') IS NOT NULL
	BEGIN
	DROP TABLE #ReAd
	End
	

	SELECT
		RA.IDX_VisitNum
	   ,S.PAT_ENC_CSN_ID
	   ,RA.RA_PrimaryPatEncCSNID
	   ,CASE WHEN RA.DaysBetweenVisits <= 7 THEN 1
			 ELSE 0
		END 'HospitalWide_7DayReadmission'
	   ,RA.HospitalWide_30DayReadmission_NUM
	INTO
		#ReAd
	FROM
		(
		  SELECT
			PAT_ENC_CSN_ID
		  FROM
			#Source
		  GROUP BY
			PAT_ENC_CSN_ID
		) S
	JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID
	JOIN RADB.dbo.ReadmissionFact RA ON RA.IDX_VisitNum = hsp.HSP_ACCOUNT_ID
										AND RA.HospitalWide_30DayReadmission_NUM = 1

										


	/*****************************************************************************************************************

	ICU

	-Get the encounters of the set in which they went to the ICU, get initial transfer in time and last transfer out time

	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#ICU') IS NOT NULL
	BEGIN
	DROP TABLE #ICU
	End
	
		;WITH Encounters AS
		(
			SELECT
				S.PAT_ENC_CSN_ID
			   ,hsp.HSP_ACCOUNT_ID
			   ,1 AS 'ICU_Admit'
			--   ,lh.Department_ID
			--   ,lh.Department_Name
			--INTO #ICU
			FROM
				RADB.dbo.vw_LocHierarchy_Department lh
			JOIN Clarity.dbo.F_IP_HSP_TRANSFER tfr ON lh.Department_ID = tfr.FROM_DEPT_ID
			JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.PAT_ENC_CSN_ID = tfr.PAT_ENC_CSN_ID
			JOIN #Source s ON s.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
			WHERE
				ICU_Department_YN = 'Y'
				--AND hsp.HOSP_DISCH_TIME >= '1/1/2014'
			GROUP BY
				S.PAT_ENC_CSN_ID
			   ,hsp.HSP_ACCOUNT_ID
			--  ,lh.Department_ID
			--   ,lh.Department_Name
		)
		,RAEncounters AS
			(
			SELECT
				S.PAT_ENC_CSN_ID
			   ,hsp.HSP_ACCOUNT_ID
			   ,1 AS 'ICU_Admit'
			--   ,lh.Department_ID
			--   ,lh.Department_Name
			--INTO #ICU
			FROM
				RADB.dbo.vw_LocHierarchy_Department lh
			JOIN Clarity.dbo.F_IP_HSP_TRANSFER tfr ON lh.Department_ID = tfr.FROM_DEPT_ID
			JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.PAT_ENC_CSN_ID = tfr.PAT_ENC_CSN_ID
			JOIN #ReAd s ON s.RA_PrimaryPatEncCSNID = hsp.PAT_ENC_CSN_ID
			WHERE
				ICU_Department_YN = 'Y'
				--AND hsp.HOSP_DISCH_TIME >= '1/1/2014'
			GROUP BY
				S.PAT_ENC_CSN_ID
			   ,hsp.HSP_ACCOUNT_ID
			--  ,lh.Department_ID
			--   ,lh.Department_Name
	
			)
		SELECT E.PAT_ENC_CSN_ID
			  ,E.HSP_ACCOUNT_ID
			  ,ISNULL(E.ICU_Admit,ra.ICU_Admit) 'ICU_Admit'
		INTO #ICU
		FROM Encounters E
		LEFT JOIN RAEncounters RA ON e.PAT_ENC_CSN_ID = RA.PAT_ENC_CSN_ID

		SELECT * FROM RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact

	/*****************************************************************************************************************
	
	Create The Outcome Metric Source
	
	*****************************************************************************************************************/

	IF OBJECT_ID(N'RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact') IS NOT NULL
	BEGIN
	DROP TABLE RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact
	End
	

	SELECT
	 S.PAT_ENC_CSN_ID
	 ,s.proceduretype
	 ,s.SURGERY_DATE
	 ,RADb.dbo.fn_Generate_DateKey(s.SURGERY_DATE) 'Surgery_DateKey'
	 ,s.PROV_ID
	 ,s.PROV_NAME 'Surgeon'
	 ,S.HOSP_ADMSN_TIME
	 ,RADb.dbo.fn_Generate_DateKey(s.HOSP_ADMSN_TIME) 'Admit_DateKey'
	 ,S.HOSP_DISCH_TIME
	 ,RADb.dbo.fn_Generate_DateKey(s.HOSP_DISCH_TIME) 'Discharge_DateKey'
	 ,S.LOS_Hours
	 ,S.LOS_Days
	 ,SUM(CASE WHEN ed.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END) 'ED_revisit'
	 ,SUM(ISNULL(RePo.ReProcedureCase,0)) 'ReProcedured'
	 ,Max(ISNULL(ICU.ICU_Admit,0)) 'ICU_Admit'
	 ,MAX(ISNULL(rad.HospitalWide_7DayReadmission,0)) 'HospitalWide_7DayReadmission'
	 ,MAX(ISNULL(rad.HospitalWide_30DayReadmission_NUM,0)) 'HospitalWide_30DayReadmission'
	 ,MAX(ISNULL(qvi.Qvi_Value,0)) 'QVI_Value'
	 ,MAX(Cost.TotalDirectCost) 'TotalDirectCost'
	INTO RADB.dbo.CRD_ERAS_YNHOBGYN_31893_OutcomeFact
	FROM #Source S
	LEFT JOIN #EDRevisit ED ON ED.Idx_CSN = S.PAT_ENC_CSN_ID
	LEFT JOIN #RePo RePo ON S.PAT_ENC_CSN_ID = RePo.Idx_CSN
	LEFT JOIN #ICU ICU ON ICU.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID
	LEFT JOIN #ReAd Rad ON Rad.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID
	LEFT JOIN #QVI QVI ON QVI.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID
	LEFT JOIN #Cost cost ON cost.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID

	GROUP BY
	  S.PAT_ENC_CSN_ID
	 ,s.proceduretype
	 ,s.SURGERY_DATE
	 ,RADb.dbo.fn_Generate_DateKey(s.SURGERY_DATE) 
	 ,s.PROV_ID
	 ,s.PROV_NAME 
	 ,S.HOSP_ADMSN_TIME
	 ,RADb.dbo.fn_Generate_DateKey(s.HOSP_ADMSN_TIME) 
	 ,S.HOSP_DISCH_TIME
	 ,RADb.dbo.fn_Generate_DateKey(s.HOSP_DISCH_TIME) 
	 ,S.LOS_Hours
	 ,S.LOS_Days




/*****************************************************************************************************************
	
Process Metrics ERAS
	
*****************************************************************************************************************/

	/*****************************************************************************************************************

	Pre Op Flowsheet

	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#PreOp') IS NOT NULL
	BEGIN
	DROP TABLE #PreOp
	End
	

	SELECT  
	--		b.LOG_ID
		b.PAT_ENC_CSN_ID AS pat_enc_csn_id
	   ,ifm.FSD_ID
	   ,ifm.LINE
	   ,ifgd.FLO_MEAS_NAME
	   ,ifm.FLO_MEAS_ID
	   ,ifgd.DISP_NAME AS Flowsheet_DisplayName
	   ,ifm.MEAS_VALUE
	   ,CASE WHEN zvt.NAME = 'Date' THEN DATEADD(DAY, CAST(ifm.MEAS_VALUE AS NUMERIC), '12/31/1840')
			 WHEN zvt.NAME = 'Time' THEN DATEADD(SECOND, CAST(ifm.MEAS_VALUE AS NUMERIC), 0)
		END AS 'ConvertedVal'
	--,		MEAS_NUMERIC=CAST(NULL AS NUMERIC(1000,4))
	   ,ifm.MEAS_COMMENT
	   ,ifm.RECORDED_TIME
	   ,ifm.ENTRY_TIME
	   ,ifm.ENTRY_USER_ID
	   ,empent.NAME AS Entry_Username
	   ,ifm.TAKEN_USER_ID
	   ,emptaken.NAME AS Taken_Username
	   ,ifgd.DUPLICATEABLE_YN
	   ,zvt.NAME AS ValueType
	   ,zrt.NAME AS RowType
	   ,b.PAT_MRN_ID

	   
	INTO
		#PreOp
	FROM
		Clarity.dbo.IP_DATA_STORE AS ids --clarity.dbo.pat_enc_hsp AS ids
	JOIN (
		   SELECT
			PAT_ENC_CSN_ID
		   ,PAT_MRN_ID
		   FROM
			#Source
		   GROUP BY
			PAT_ENC_CSN_ID
		   ,PAT_MRN_ID
		 ) b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID
	LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
	LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
	LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
	LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
	LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
	LEFT JOIN Clarity.dbo.CLARITY_EMP AS emptaken ON emptaken.USER_ID = ifm.TAKEN_USER_ID
	LEFT JOIN Clarity.dbo.CLARITY_EMP AS empent ON empent.USER_ID = ifm.ENTRY_USER_ID
	WHERE
		ifm.FLO_MEAS_ID IN ( '1020100004'/*Date Last Liquid*/
												, '1217'/*Time Last Liquid*/
												, '1077096112'/*Last Liquid*/
												, '10713938'/*Pre-Op Call ERAS Pamphlet*/	
												, '39164'/*Anthithrombotic taken in last 7 days*/
												, '30469167'/*Last date Anthithrombotic taken date*/
												, '30469168'/*Last date Anthithrombotic taken time*/
												, '30439165'/*Has anesthesia been notified of last dose of antithrombotic*/
												, '30439169' /*Anesthesia Provider notified*/
												)


												
		AND ifm.MEAS_VALUE IS NOT NULL 




	 
	--SELECT TOP 0
	--s.PAT_ENC_CSN_ID
	--,T.* 
	--,0 PreOp_ERASProcedure
	--,0 PreOp_Pamphlet
	--,0 PreOp_LastLiquid_Documented
	--,0 PreOp_Gatorade_AppleJuice
	--,0 PreOp_AnthithromboticTaken
	--,0 PreOp_LastDateAnthithromboticTaken
	--,0 PreOp_AnthithromboticAnesthesiaNotify
	--,0 PreOp_AnesthesiaDrNotify
	--INTO RADB.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics
	--FROM
	-- #Source s
	--JOIN RADB.[dbo].[CRD_ERAS_Global_TimeWindows] T ON T.LOG_ID = s.LOG_ID 
	 

	-- PreOp_ERASProcedure
	--,PreOp_Pamphlet
	--,PreOp_LastLiquid_Documented
	--,PreOp_Gatorade_AppleJuice
	--,PreOp_AnthithromboticTaken
	--,PreOp_LastDateAnthithromboticTaken
	--,PreOp_AnthithromboticAnesthesiaNotify
	--,PreOp_AnesthesiaDrNotify


	/*****************************************************************************************************************
	
	Clean out the Proccess Metrics Table and load in all cases
	
	*****************************************************************************************************************/

	IF OBJECT_ID(N'RADB.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics') IS NOT NULL
	BEGIN
	TRUNCATE TABLE RADB.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics
	End
	


	INSERT	INTO RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics]
			( PAT_ENC_CSN_ID
			,LOG_ID
			,Surgery_Date
			,ProcedureType
			,PROV_ID
			,OUT_OF_ROOM
			,IN_PACU
			,OUT_OF_PACU
			,IN_PRE_PROCEDURE
			,PROCEDURE_FINISH
			,PRE_PROCEDURE_COMPLETE
			,IN_ROOM
			,ANESTHESIA_START
			,PROCEDURE_START 
			)
			SELECT
				s.PAT_ENC_CSN_ID
			   ,T.LOG_ID
			   ,s.SURGERY_DATE
			   ,s.ProcedureType
			   ,s.PROV_ID
			   ,T.OUT_OF_ROOM
			   ,T.IN_PACU
			   ,T.OUT_OF_PACU
			   ,T.IN_PRE_PROCEDURE
			   ,T.PROCEDURE_FINISH
			   ,T.PRE_PROCEDURE_COMPLETE
			   ,T.IN_ROOM
			   ,T.ANESTHESIA_START
			   ,T.PROCEDURE_START
			FROM
				#Source s
			JOIN RADB.[dbo].[CRD_ERAS_Global_TimeWindows] T ON T.LOG_ID = s.LOG_ID
			--WHERE s.PAT_ENC_CSN_ID = 117478842
			--AND s.LOG_ID = 443053
			/*Need to apply a group by to eliminate duplicate logs.  There are duplicate logs for patients that had two procedures done but at the same time creating one single log*/
			GROUP BY
				s.PAT_ENC_CSN_ID
			   ,T.LOG_ID
			   ,s.SURGERY_DATE
			   ,s.ProcedureType
			   ,s.PROV_ID
			   ,T.OUT_OF_ROOM
			   ,T.IN_PACU
			   ,T.OUT_OF_PACU
			   ,T.IN_PRE_PROCEDURE
			   ,T.PROCEDURE_FINISH
			   ,T.PRE_PROCEDURE_COMPLETE
			   ,T.IN_ROOM
			   ,T.ANESTHESIA_START
			   ,T.PROCEDURE_START




	/*****************************************************************************************************************
	
	Last Liquid 
	
	*****************************************************************************************************************/


	UPDATE pm
	SET PreOp_LastLiquid_Documented = ISNULL(Measure,0)
	--SELECT *
	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	JOIN radb.dbo.CRD_ERAS_Global_TimeWindows_vw tw ON pm.LOG_ID = tw.LOG_ID
	LEFT JOIN
	 (
				SELECT
					pat_enc_csn_id
					,RECORDED_TIME
					,1 'Measure'
				FROM
					#PreOp
				WHERE
					FLO_MEAS_ID IN ( 
									'1020100004'/*Date Last Liquid*/
									,'1217' /*Time Last Liquid*/
									)
				GROUP BY pat_enc_csn_id
						,RECORDED_TIME
		) LQ ON LQ.pat_enc_csn_id = pm.pat_enc_csn_id
			AND LQ.RECORDED_TIME BETWEEN PreOp_Start AND PreOp_End


	/*****************************************************************************************************************
	
	ERAS In Procedure Description
	
	*****************************************************************************************************************/


	UPDATE pm
	SET  PreOp_ERASProcedure = ISNULL(Measure,0)
	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	LEFT JOIN
	 (
				SELECT
					pat_enc_csn_id
					,1 'Measure'
				FROM
					#Source
				WHERE
					PROC_DISPLAY_NAME LIKE '%ERAS%'
				GROUP BY pat_enc_csn_id
		) LQ ON LQ.pat_enc_csn_id = pm.pat_enc_csn_id	


	/*****************************************************************************************************************
	
	Pamphlet Given 
	
	*****************************************************************************************************************/


	UPDATE pm
	SET PreOp_Pamphlet = ISNULL(Measure,0)
	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	LEFT JOIN
	 (
				SELECT
					pat_enc_csn_id
					,1 'Measure'
				FROM
					#PreOp
				WHERE
					FLO_MEAS_ID IN ( 
									'10713938'/*Pre-Op Call ERAS Pamphlet*/
									)
				GROUP BY pat_enc_csn_id
		) LQ ON LQ.pat_enc_csn_id = pm.pat_enc_csn_id


	/*****************************************************************************************************************
	
	Gatorade/Apple Juice Intake
	
	*****************************************************************************************************************/


	UPDATE pm
	SET PreOp_Gatorade_AppleJuice = ISNULL(Measure,0)
	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	LEFT JOIN
	 (
				SELECT
					pat_enc_csn_id
					,1 'Measure'

				FROM
					#PreOp
				WHERE
					FLO_MEAS_ID IN ( 
									'1077096112'/*Last Liquid Gatorade/Apple Juice*/
									)
					AND (meas_Value LIKE '%Gatorade%' OR  meas_Value LIKE '%Apple%')
				GROUP BY pat_enc_csn_id
		) LQ ON LQ.pat_enc_csn_id = pm.pat_enc_csn_id


	/*****************************************************************************************************************
	
	Antithrombotic
	
	*****************************************************************************************************************/


	UPDATE pm
	SET pm.PreOp_AnthithromboticTaken = ISNULL(Anthithrombotic,0)
	,pm.PreOp_LastDateAnthithromboticTaken = ISNULL(AnthithromboticDate,0)
	,pm.PreOp_AnthithromboticAnesthesiaNotify = ISNULL(anesthesiaNotify,0)
	,pm.PreOp_AnesthesiaDrNotify = ISNULL(anesthesiaNotify,0)

	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	LEFT JOIN
	 (
				SELECT
					pat_enc_csn_id
					,MAX(CASE WHEN FLO_MEAS_ID = '39164' THEN 1 ELSE 0 END) 'Anthithrombotic'
					,MAX(CASE WHEN FLO_MEAS_ID = '30469167' OR FLO_MEAS_ID = '30469168'  THEN 1 ELSE 0 END) 'AnthithromboticDate'
					,MAX(CASE WHEN FLO_MEAS_ID = '30439165' THEN 1 ELSE 0 END) 'anesthesiaNotify'
					,MAX(CASE WHEN FLO_MEAS_ID = '30439169' THEN 1 ELSE 0 END) 'AnesthesiaProviderNotified'

				FROM
					#PreOp
				WHERE
					FLO_MEAS_ID IN ( 
									'39164'/*Anthithrombotic taken in last 7 days*/
									, '30469167'/*Last date Anthithrombotic taken date*/
									, '30469168'/*Last date Anthithrombotic taken time*/
									, '30439165'/*Has anesthesia been notified of last dose of antithrombotic*/
									, '30439169' /*Anesthesia Provider notified*/
									)

				GROUP BY pat_enc_csn_id
		) LQ ON LQ.pat_enc_csn_id = pm.pat_enc_csn_id



	/*****************************************************************************************************************

	Intra Op

	*****************************************************************************************************************/

	/*****************************************************************************************************************

	Post Op

	*****************************************************************************************************************/

	IF OBJECT_ID(N'TEMPDB..#PostOp') IS NOT NULL
	BEGIN
	DROP TABLE #PostOp
	End
	


	SELECT  
	--		b.LOG_ID
			b.PAT_ENC_CSN_ID AS pat_enc_csn_id
	,       ifm.FSD_ID
	,		ifm.line
	,       ifgd.FLO_MEAS_NAME
	,		ifm.FLO_MEAS_ID
	,       ifgd.DISP_NAME AS Flowsheet_DisplayName
	,		ifm.MEAS_VALUE
	,       CASE WHEN zvt.name = 'Date' Then DATEADD(DAY,CAST(ifm.MEAS_VALUE as NUMERIC),'12/31/1840')
				 WHEN zvt.name = 'Time' Then DATEADD(SECOND,CAST(ifm.MEAS_VALUE as NUMERIC),0) 
				 END AS  'ConvertedVal'
	--,		MEAS_NUMERIC=CAST(NULL AS NUMERIC(1000,4))
	,       ifm.MEAS_COMMENT
	,       ifm.RECORDED_TIME
	,		ifm.ENTRY_TIME
	,		ifm.ENTRY_USER_ID
	,		empent.NAME AS Entry_Username
	,		ifm.TAKEN_USER_ID
	,		emptaken.NAME AS Taken_Username
	,       ifgd.DUPLICATEABLE_YN
	,       zvt.name AS ValueType
	,       zrt.name AS RowType
	,		b.pat_mrn_id


	INTO #PostOp
	FROM    clarity.dbo.IP_DATA_STORE AS ids
			--clarity.dbo.pat_enc_hsp AS ids
			JOIN #Source b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID
			LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
			LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
			LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
			LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
			LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
			LEFT JOIN clarity.dbo.CLARITY_EMP AS emptaken ON emptaken.USER_ID=ifm.TAKEN_USER_ID
			LEFT JOIN clarity.dbo.CLARITY_EMP AS empent ON empent.USER_ID=ifm.ENTRY_USER_ID
		
			WHERE          ifm.FLO_MEAS_ID IN ( 
												 '304625312' /*Gum Chewed*/
												  
												,'51' /*Liquid Consumed*/
												,'4515' /*Diet/Feeding Tolerance*/
												,'5966' /*% of Meal Consumed*/
												,'304340' /*Stool Occurance*/
												,'304351' /*Flatus Occurence*/
												, '8148', '8151' /*'3048148000'*/ /*Foley Cath*/
												, '3046874'   /*Ambulation distance*/
												, '3047745' /*Gait PT Ambulation*/
											    , '3040102774' /*post void residual cath*/
												 
												)		        
			AND ifm.MEAS_VALUE IS NOT NULL 


	/*****************************************************************************************************************
	
	Post Op Metrics
	
	*****************************************************************************************************************/
	
	UPDATE pm
	SET 
	--Select
		pm.PostOp_D0_FluidIntake = ISNULL(FL.PostOp_D0_FluidIntake, 0)
		,pm.PostOp_D0_GumChewed=isnull(FL.PostOp_D0_GumChewed, 0)
		,pm.PostOp_D0_DietTolerance=isnull(FL.PostOp_D0_DietTolerance, 0)
		--,pm.PostOp_D0_DiscontinuedIV=isnull(FL.PostOp_D0_DiscontinuedIV, 0)
		,pm.PostOp_D0_StoolOccurrence=isnull(FL.PostOp_D0_StoolOccurrence, 0)
		--,pm.PostOp_D0_Ambulation=isnull(FL.PostOp_D0_Ambulation, 0)
		,pm.PostOp_D1_FluidIntake=isnull(FL.PostOp_D1_FluidIntake, 0)
		,pm.PostOp_D1_SolidIntake=isnull(FL.PostOp_D1_SolidIntake, 0)
		,pm.PostOp_D1_GumChewed=isnull(FL.PostOp_D1_GumChewed, 0)
		,pm.PostOp_D1_StoolOccurrence=isnull(FL.PostOp_D1_StoolOccurrence, 0)
		--,pm.PostOp_D1_FoleyRemoved=isnull(FL.PostOp_D1_FoleyRemoved, 0)
		--,pm.PostOp_D1_Ambulation=isnull(FL.PostOp_D1_Ambulation, 0)
		--,pm.PostOp_D2_SolidIntake=isnull(FL.PostOp_D2_SolidIntake, 0) /*Excluding this one as it is captured in the Diet Tolerence*/
		,pm.PostOp_D2_11AmDischarge=isnull(FL.PostOp_D2_11AmDischarge, 0)
		,pm.PostOp_D2_DischargeAfter11AM=isnull(FL.PostOp_D2_DischargeAfter11AM, 0)
		,pm.PostOp_D2_GumChewed=isnull(FL.PostOp_D2_GumChewed, 0)
		,pm.PostOp_D2_FluidIntake=isnull(FL.PostOp_D2_FluidIntake, 0)
		,pm.PostOp_D2_DietTolerance=isnull(FL.PostOp_D2_DietTolerance, 0)
		,pm.PostOp_D2_StoolOccurrence=isnull(FL.PostOp_D2_StoolOccurrence, 0)
		--,pm.PostOp_D2_Ambulation=isnull(FL.PostOp_D2_Ambulation, 0)

	FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
	LEFT JOIN
	 (
	 				SELECT
					po.pat_enc_csn_id
					,pm.log_id
					,MAX(CASE WHEN FLO_MEAS_ID = '51' AND RECORDED_TIME BETWEEN times.postOp_start AND times.PostOp_D1 THEN 1 ELSE 0 END) 'PostOp_D0_FluidIntake'
					,MAX(CASE WHEN FLO_MEAS_ID = '304625312' 
									AND RECORDED_TIME BETWEEN times.postOp_start AND times.PostOp_D1 
									AND meas_Value = 'Yes' THEN 1 ELSE 0 END) 'PostOp_D0_GumChewed'

					,MAX(CASE WHEN FLO_MEAS_ID = '4515' 
									AND RECORDED_TIME BETWEEN times.postOp_start AND times.PostOp_D1  THEN 1 ELSE 0 END) 'PostOp_D0_DietTolerance'

					,MAX(CASE WHEN FLO_MEAS_ID = '304340' 
									AND RECORDED_TIME BETWEEN times.postOp_start AND times.PostOp_D1  THEN 1 ELSE 0 END) 'PostOp_D0_StoolOccurrence'

					,MAX(CASE WHEN FLO_MEAS_ID = '51' 
									AND RECORDED_TIME BETWEEN times.PostOp_D1 AND times.PostOp_D2 THEN 1 ELSE 0 END) 'PostOp_D1_FluidIntake'

					,MAX(CASE WHEN FLO_MEAS_ID = '5966' 
									AND RECORDED_TIME BETWEEN times.PostOp_D1 AND times.PostOp_D2 THEN 1 ELSE 0 END) 'PostOp_D1_SolidIntake'

					,MAX(CASE WHEN FLO_MEAS_ID = '304625312' 
									AND RECORDED_TIME BETWEEN times.PostOp_D1 AND times.PostOp_D2
									AND meas_Value = 'Yes' THEN 1 ELSE 0 END) 'PostOp_D1_GumChewed'
					,MAX(CASE WHEN FLO_MEAS_ID = '304340' 
									AND RECORDED_TIME BETWEEN times.PostOp_D1 AND times.PostOp_D2  THEN 1 ELSE 0 END) 'PostOp_D1_StoolOccurrence'

					--,MAX(CASE WHEN FLO_MEAS_ID = '5966' 
					--				AND RECORDED_TIME BETWEEN times.PostOp_D2 AND times.PostOp_D3 
					--				AND CAST(
					--				CASE WHEN ISNUMERIC( REPLACE(po.MEAS_VALUE,'%','') ) = 0 THEN 0 ELSE REPLACE(po.MEAS_VALUE,'%','') END /*Bad Documentation by some nurses causes us to have check if the entered value can be converted.  If it can not we give them a 0*/
					--						 AS int) >= 75 THEN 1 ELSE 0 END) 'PostOp_D2_SolidIntake'

					,MAX(CASE WHEN CAST(hsp.HOSP_DISCH_TIME AS DATE) = times.PostOp_D2 
									AND DATEPART(HOUR,hsp.HOSP_DISCH_TIME) < 11 THEN 1 ELSE 0 END) 'PostOp_D2_11AmDischarge'

					,MAX(CASE WHEN CAST(hsp.HOSP_DISCH_TIME AS DATE) >= times.PostOp_D2 
									AND DATEPART(HOUR,hsp.HOSP_DISCH_TIME) > 11 THEN 1 ELSE 0 END) 'PostOp_D2_DischargeAfter11AM'

					,MAX(CASE WHEN FLO_MEAS_ID = '304625312' 
									AND CAST(hsp.HOSP_DISCH_TIME AS DATE) = times.PostOp_D2 
									AND DATEPART(HOUR,hsp.HOSP_DISCH_TIME) >= 11
									AND RECORDED_TIME BETWEEN times.PostOp_D2 AND times.PostOp_D3
									AND meas_Value = 'Yes' THEN 1 ELSE 0 END) 'PostOp_D2_GumChewed'

					,MAX(CASE WHEN FLO_MEAS_ID = '51' AND RECORDED_TIME BETWEEN times.PostOp_D2 AND times.PostOp_D3 THEN 1 ELSE 0 END) 'PostOp_D2_FluidIntake'

					,MAX(CASE WHEN FLO_MEAS_ID = '5966' 
									AND RECORDED_TIME BETWEEN times.PostOp_D2 AND times.PostOp_D3 
									AND CAST(
									CASE WHEN ISNUMERIC( REPLACE(po.MEAS_VALUE,'%','') ) = 0 THEN 0 ELSE REPLACE(po.MEAS_VALUE,'%','') END /*Bad Documentation by some nurses causes us to have check if the entered value can be converted.  If it can not we give them a 0*/
											 AS int) >= 75 THEN 1 ELSE 0 END) 'PostOp_D2_DietTolerance'


					,MAX(CASE WHEN FLO_MEAS_ID = '304340' 
									AND RECORDED_TIME BETWEEN times.PostOp_D3 AND hsp.HOSP_DISCH_TIME  THEN 1 ELSE 0 END) 'PostOp_D2_StoolOccurrence'

					--,MAX(CASE WHEN FLO_MEAS_ID = '30439165' THEN 1 ELSE 0 END) 'anesthesiaNotify'
					--,MAX(CASE WHEN FLO_MEAS_ID = '30439169' THEN 1 ELSE 0 END) 'AnesthesiaProviderNotified'

				FROM
					 RADB.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics pm
					 JOIN Clarity.dbo.PAT_ENC_HSP hsp ON hsp.PAT_ENC_CSN_ID = pm.PAT_ENC_CSN_ID
					 JOIN  RADB.dbo.CRD_ERAS_Global_TimeWindows_vw times ON times.log_id = pm.Log_ID
					 JOIN #PostOp po ON pm.PAT_ENC_CSN_ID = po.pat_enc_csn_id 
				WHERE
			--	po.pat_enc_csn_id = 113305527 AND pm.LOG_ID IN  ('394204', '398663') and
					FLO_MEAS_ID IN ( 
									'304625312' /*Gum Chewed*/
									,'51' /*Liquid Consumed*/
									,'4515' /*Diet/Feeding Tolerance*/
									,'5966' /*% of Meal Consumed*/
									,'304340' /*Stool Occurance*/
									,'304351' /*Flatus Occurence*/
									, '8148', '8151' /*'3048148000'*/ /*Foley Cath*/
									, '3046874'   /*Ambulation distance*/
									, '3040102774' /*post void residual cath*/
									)

				GROUP BY po.pat_enc_csn_id
				,pm.LOG_ID
		) FL ON FL.pat_enc_csn_id = pm.pat_enc_csn_id AND fl.LOG_ID = pm.LOG_ID


		


	/*****************************************************************************************************************

	Ambulation

	*****************************************************************************************************************/

		;WITH baseamb AS (
		SELECT 
			s.pat_enc_csn_id
			,v.Value
			,s.fsd_id
			,s.line
			,s.RECORDED_TIME
			,Ambulate_In_room = CASE WHEN RTRIM(LTRIM(value)) = 'ambulate in room' THEN 1 ELSE 0 END
			,Ambulate_In_Hall = CASE WHEN RTRIM(LTRIM(value)) = 'ambulate in hall' THEN 1 ELSE 0 END
			,pt_bedtochair =  CASE WHEN RTRIM(LTRIM(value)) = 'bed to chair' THEN 1 ELSE 0 END
			,pt_chairtobed =  CASE WHEN RTRIM(LTRIM(value)) = 'chair to bed' THEN 1 ELSE 0 END
			,pt_sidesteps =  CASE WHEN RTRIM(LTRIM(value)) = 'sidesteps' THEN 1 ELSE 0 END
			,pt_5ft =  CASE WHEN RTRIM(LTRIM(value)) = '5 feet' THEN 1 ELSE 0 END
			,pt_10ft =  CASE WHEN RTRIM(LTRIM(value)) = '10 feet' THEN 1 ELSE 0 END
			,pt_15ft =  CASE WHEN RTRIM(LTRIM(value)) = '15 feet' THEN 1 ELSE 0 END
			,pt_20ft =  CASE WHEN RTRIM(LTRIM(value)) = '20 feet' THEN 1 ELSE 0 END
			,pt_25ft =  CASE WHEN RTRIM(LTRIM(value)) = '25 feet' THEN 1 ELSE 0 END
			,pt_50ft =  CASE WHEN RTRIM(LTRIM(value)) = '50 feet' THEN 1 ELSE 0 END
			,pt_75ft =  CASE WHEN RTRIM(LTRIM(value)) = '75 feet' THEN 1 ELSE 0 END
			,pt_100ft =  CASE WHEN RTRIM(LTRIM(value)) = '100 feet' THEN 1 ELSE 0 END
			,pt_150ft =  CASE WHEN RTRIM(LTRIM(value)) = '150 feet' THEN 1 ELSE 0 END
			,pt_200ft =  CASE WHEN RTRIM(LTRIM(value)) = '200 feet' THEN 1 ELSE 0 END
			,pt_250ft =  CASE WHEN RTRIM(LTRIM(value)) = '250 feet' THEN 1 ELSE 0 END
			,pt_300ft =  CASE WHEN RTRIM(LTRIM(value)) = '300 feet' THEN 1 ELSE 0 END
			,pt_350ft =  CASE WHEN RTRIM(LTRIM(value)) = '350 feet' THEN 1 ELSE 0 END
			,pt_400ft =  CASE WHEN RTRIM(LTRIM(value)) = '400 feet' THEN 1 ELSE 0 END
			,pt_x2 =  CASE WHEN RTRIM(LTRIM(value)) = 'x2' THEN 1 ELSE 0 END
			,pt_x3 =  CASE WHEN RTRIM(LTRIM(value)) = 'x3' THEN 1 ELSE 0 END

			  FROM #PostOp s
				CROSS APPLY radb.dbo.YNHH_SplitToTable(meas_value,';') AS v
			WHERE s.FLO_MEAS_ID in ('3046874','3047745')
			--AND pat_enc_csn_id = 113721012
		)
		, rolled AS 
		(
			SELECT
			r.pat_enc_csn_id
			,s.LOG_ID
			--,r.RECORDED_TIME
			--fsd_id,
			--line,
			,Ambulate_In_room = SUM(Ambulate_In_room)
			,Ambulate_In_Hall = SUM(Ambulate_In_Hall)
			,pt_bedtochair = SUM(pt_bedtochair)
			,pt_chairtobed = SUM(pt_chairtobed)
			,pt_sidesteps = SUM(pt_sidesteps)
			,pt_5ft = SUM(pt_5ft)
			,pt_10ft = SUM(pt_10ft)
			,pt_15ft = SUM(pt_15ft)
			,pt_20ft = SUM(pt_20ft)
			,pt_25ft = SUM(pt_25ft)
			,pt_50ft = SUM(pt_50ft)
			,pt_75ft = SUM(pt_75ft)
			,pt_100ft = SUM(pt_100ft)
			,pt_150ft = SUM(pt_150ft)
			,pt_200ft = SUM(pt_200ft)
			,pt_250ft = SUM(pt_250ft)
			,pt_300ft = SUM(pt_300ft)
			,pt_350ft = SUM(pt_350ft)
			,pt_400ft = SUM(pt_400ft)
			,pt_x2 = SUM(pt_x2)
			,pt_x3 = SUM(pt_x3)

		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.PreOp_Start AND t.PreOp_End THEN 1 ELSE 0 END) 'PreOp'
		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.IntraOp_Start AND t.IntraOp_End THEN 1 ELSE 0 END)'IntraOp'
		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.PACU_In AND t.PACU_OUT THEN 1 ELSE 0 END) 'PACU'
		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_Start AND t.PostOp_D1 THEN 1 ELSE 0 END) 'PostOp_D0'
		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D1 AND t.PostOp_D2 THEN 1 ELSE 0 END) 'PostOp_D1' 
		   --,MAX(CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D2 AND CASE WHEN s.HOSP_DISCH_TIME < t.PostOp_D3 THEN s.HOSP_DISCH_TIME ELSE PostOp_D3 END THEN 1 ELSE 0 END) 'PostOp_D2'

		   	,CASE WHEN r.RECORDED_TIME BETWEEN t.PreOp_Start AND t.PreOp_End THEN 1 ELSE 0 END 'PreOp'
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.IntraOp_Start AND t.IntraOp_End THEN 1 ELSE 0 END 'IntraOp'
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PACU_In AND t.PACU_OUT THEN 1 ELSE 0 END 'PACU'
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_Start AND t.PostOp_D1 THEN 1 ELSE 0 END 'PostOp_D0'
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D1 AND t.PostOp_D2 THEN 1 ELSE 0 END 'PostOp_D1' 
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D2 AND CASE WHEN s.HOSP_DISCH_TIME < t.PostOp_D3 THEN s.HOSP_DISCH_TIME ELSE PostOp_D3 END THEN 1 ELSE 0 END 'PostOp_D2'
			FROM
			baseamb r
				JOIN #Source s ON s.PAT_ENC_CSN_ID = r.pat_enc_csn_id
				JOIN radb.dbo.CRD_ERAS_Global_TimeWindows_vw t ON t.LOG_ID = s.LOG_ID

			--	WHERE r.pat_enc_csn_id = 117332401
			/*using this ugly group by so I can maintain if something happened more than once durring a given period.*/
			GROUP BY
			r.pat_enc_csn_id
			,s.LOG_ID

		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PreOp_Start AND t.PreOp_End THEN 1 ELSE 0 END 
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.IntraOp_Start AND t.IntraOp_End THEN 1 ELSE 0 END 
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PACU_In AND t.PACU_OUT THEN 1 ELSE 0 END 
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_Start AND t.PostOp_D1 THEN 1 ELSE 0 END 
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D1 AND t.PostOp_D2 THEN 1 ELSE 0 END  
		   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D2 AND CASE WHEN s.HOSP_DISCH_TIME < t.PostOp_D3 THEN s.HOSP_DISCH_TIME ELSE PostOp_D3 END THEN 1 ELSE 0 END 

			--,r.RECORDED_TIME
		)

		--SELECT * FROM rolled WHERE rolled.PostOp_D0 = 1

		UPDATE pm
		SET 

		pm.PostOp_D0_Ambulation=isnull(a.PostOp_D0_Ambulation, 0)

		,pm.PostOp_D1_Ambulation=isnull(a.PostOp_D1_Ambulation, 0)

		,pm.PostOp_D2_Ambulation=isnull(a.PostOp_D2_Ambulation, 0)

		--SELECT a.*

		FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
		LEFT JOIN 
		(
		SELECT
		 am.pat_enc_csn_id
		,am.LOG_ID
		,CASE WHEN am.PostOp_D0 = 1 
					AND (am.Ambulate_In_room + 	am.Ambulate_In_Hall + 	am.pt_bedtochair + 	am.pt_chairtobed + 	am.pt_sidesteps + 	am.pt_5ft + 	am.pt_10ft + 	am.pt_15ft + 	am.pt_20ft + 	am.pt_25ft + 	am.pt_50ft + 	am.pt_75ft + 	am.pt_100ft + 	am.pt_150ft + 	am.pt_200ft + 	am.pt_250ft + 	am.pt_300ft + 	am.pt_350ft + 	am.pt_400ft + 	am.pt_x2 + 	am.pt_x3  ) 
					> 0 THEN 1 ELSE 0 END 'PostOp_D0_Ambulation'
		
		,CASE WHEN am.PostOp_D1 = 1 
					AND (am.pt_50ft + 	am.pt_75ft + 	am.pt_100ft + 	am.pt_150ft + 	am.pt_200ft + 	am.pt_250ft + 	am.pt_300ft + 	am.pt_350ft + 	am.pt_400ft + 	am.pt_x2 + 	am.pt_x3  ) 
					> 1 THEN 1 ELSE 0 END 'PostOp_D1_Ambulation'
		,CASE WHEN am.PostOp_D2 = 1
					AND (am.pt_50ft + 	am.pt_75ft + 	am.pt_100ft + 	am.pt_150ft + 	am.pt_200ft + 	am.pt_250ft + 	am.pt_300ft + 	am.pt_350ft + 	am.pt_400ft + 	am.pt_x2 + 	am.pt_x3  ) 
					> 0 THEN 1 ELSE 0 END 'PostOp_D2_Ambulation'
		From
		rolled am
		) a ON a.LOG_ID = pm.LOG_ID



/*****************************************************************************************************************

Multi-Modal Medications

*****************************************************************************************************************/


	IF OBJECT_ID(N'TEMPDB..#GivenMeds') IS NOT NULL
	BEGIN
	DROP TABLE #GivenMeds
	End


	SELECT
		MAI.MAR_ENC_CSN AS pat_enc_csn_id
	   ,EO.LOG_ID
	   ,CM.MEDICATION_ID
	   ,CM.NAME AS MedicationName
	   ,CM.FORM AS MedicationForm
	   ,om.ORDER_MED_ID
	   ,MAI.LINE
	   ,rMT.MED_BRAND_NAME
	   ,EMPAdmin.USER_ID AdminId
	   ,EMPAdmin.NAME AS AdministeredBy
	   ,admindep.DEPARTMENT_NAME AS AdministeredDept
		--	,meddim.MedType
	   ,MAI.TAKEN_TIME
	   ,zCact.NAME AS MarAction
	   ,MAI.MAR_ACTION_C
	   ,MAI.SIG AS GivenDose
	   ,zMU.NAME AS DoseUnit
	   ,zAR.NAME AS Route
	   ,MAI.DOSE_UNIT_C
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PreOp_Start AND Times.PreOp_End THEN 1 ELSE 0 END 'PreOp'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.IntraOp_Start AND Times.IntraOp_End THEN 1 ELSE 0 END 'IntraOp'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PACU_In AND Times.PACU_OUT THEN 1 ELSE 0 END 'PACU'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_Start AND Times.PostOp_D1 THEN 1 ELSE 0 END 'PostOp_D0'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_D1 AND Times.PostOp_D2 THEN 1 ELSE 0 END 'PostOp_D1'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_D2 AND Times.PostOp_D3 THEN 1 ELSE 0 END 'PostOp_D2'
	   ,CASE WHEN TAKEN_TIME > Times.PostOp_D3 THEN 1 ELSE 0 END 'PostOp_D3'
	   ,admissioncsn_flag = 1
	   ,anescsn_flag = 0
				
	INTO #GivenMeds		
	FROM
		Clarity.dbo.MAR_ADMIN_INFO AS MAI
	JOIN #Source AS EO ON EO.PAT_ENC_CSN_ID = MAI.MAR_ENC_CSN
	LEFT JOIN Clarity.dbo.CLARITY_EMP AS EMPAdmin ON EMPAdmin.USER_ID = MAI.USER_ID
	LEFT JOIN Clarity.dbo.ORDER_MED AS om ON MAI.ORDER_MED_ID = om.ORDER_MED_ID
	LEFT JOIN Clarity.dbo.ZC_MED_UNIT AS zMU ON zMU.DISP_QTYUNIT_C = MAI.DOSE_UNIT_C
	LEFT JOIN Clarity.dbo.CLARITY_MEDICATION CM ON om.MEDICATION_ID = CM.MEDICATION_ID
	LEFT JOIN Clarity.dbo.CLARITY_DEP AS admindep ON admindep.DEPARTMENT_ID = MAI.MAR_ADMIN_DEPT_ID
	LEFT JOIN Clarity.dbo.RX_MED_THREE AS rMT ON rMT.MEDICATION_ID = CM.MEDICATION_ID
	LEFT JOIN RADB.dbo.CRD_ERASOrtho_Med_Dim AS MedDim ON MedDim.medication_id = CM.MEDICATION_ID
	LEFT JOIN Clarity.dbo.ZC_MAR_RSLT AS zCact ON zCact.RESULT_C = MAI.MAR_ACTION_C
	LEFT JOIN Clarity.dbo.PATIENT AS p ON om.PAT_ID = p.PAT_ID
	LEFT JOIN Clarity.dbo.ZC_ADMIN_ROUTE AS zAR ON MAI.ROUTE_C = zAR.MED_ROUTE_C
	/*Join in Time stamps for window periods*/
	JOIN RADB.dbo.CRD_ERAS_Global_TimeWindows_vw Times ON Times.LOG_ID = EO.LOG_ID
	WHERE
		MAI.MAR_ACTION_C IN ( 1, 102, 113, 118, 119, 134, 137, 142 )

	UNION ALL

	SELECT
		mai.MAR_ENC_CSN AS pat_enc_csn_id
	   ,eo.LOG_ID
	   ,cm.MEDICATION_ID
	   ,cm.NAME AS MedicationName
	   ,cm.FORM AS MedicationForm
	   ,om.ORDER_MED_ID
	   ,mai.LINE
	   ,rmt.MED_BRAND_NAME
	   ,empadmin.USER_ID AdminId
	   ,empadmin.NAME AS AdministeredBy
	   ,admindep.DEPARTMENT_NAME AS AdministeredDept
		--	,meddim.MedType
	   ,mai.TAKEN_TIME
	   ,zcact.NAME AS MarAction
	   ,mai.MAR_ACTION_C
	   ,mai.SIG AS GivenDose
	   ,zmu.NAME AS DoseUnit
	   ,zar.NAME AS Route
	   ,mai.DOSE_UNIT_C
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PreOp_Start AND Times.PreOp_End THEN 1 ELSE 0 END 'PreOp'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.IntraOp_Start AND Times.IntraOp_End THEN 1 ELSE 0 END 'IntraOp'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PACU_In AND Times.PACU_OUT THEN 1 ELSE 0 END 'PACU'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_Start AND Times.PostOp_D1 THEN 1 ELSE 0 END 'PostOp_D0'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_D1 AND Times.PostOp_D2 THEN 1 ELSE 0 END 'PostOp_D1'
	   ,CASE WHEN TAKEN_TIME BETWEEN Times.PostOp_D2 AND Times.PostOp_D3 THEN 1 ELSE 0 END 'PostOp_D2'
	   ,CASE WHEN TAKEN_TIME > Times.PostOp_D3 THEN 1 ELSE 0 END 'PostOp_D3'

	   ,admissioncsn_flag = 0
	   ,anescsn_flag = 1
	FROM
		Clarity.dbo.MAR_ADMIN_INFO AS mai
	JOIN #Source AS eo ON eo.Anes_Enc_Csn_ID = mai.MAR_ENC_CSN
	LEFT JOIN Clarity.dbo.CLARITY_EMP AS empadmin ON empadmin.USER_ID = mai.USER_ID
	LEFT JOIN Clarity.dbo.ORDER_MED AS om ON mai.ORDER_MED_ID = om.ORDER_MED_ID
	LEFT JOIN Clarity.dbo.ZC_MED_UNIT AS zmu ON zmu.DISP_QTYUNIT_C = mai.DOSE_UNIT_C
	LEFT JOIN Clarity.dbo.CLARITY_MEDICATION cm ON om.MEDICATION_ID = cm.MEDICATION_ID
	LEFT JOIN Clarity.dbo.CLARITY_DEP AS admindep ON admindep.DEPARTMENT_ID = mai.MAR_ADMIN_DEPT_ID
	LEFT JOIN Clarity.dbo.RX_MED_THREE AS rmt ON rmt.MEDICATION_ID = cm.MEDICATION_ID
	LEFT JOIN RADB.dbo.CRD_ERASOrtho_Med_Dim AS meddim ON meddim.medication_id = cm.MEDICATION_ID
	LEFT JOIN Clarity.dbo.ZC_MAR_RSLT AS zcact ON zcact.RESULT_C = mai.MAR_ACTION_C
	LEFT JOIN Clarity.dbo.PATIENT AS p ON om.PAT_ID = p.PAT_ID
	LEFT JOIN Clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C = zar.MED_ROUTE_C

	/*Join in Time stamps for window periods*/
	JOIN RADB.dbo.CRD_ERAS_Global_TimeWindows_vw Times ON Times.LOG_ID = eo.LOG_ID
	WHERE
		mai.MAR_ACTION_C IN ( 1, 102, 113, 118, 119, 134, 137, 142 )

					




	--preop multi modal anesthesia

	;WITH multibase AS(
	SELECT
		rid = ROW_NUMBER() OVER ( PARTITION BY S.LOG_ID ORDER BY S.LOG_ID )
	--,c.LOG_ID
	--   ,S.pat_name
	   ,S.PAT_MRN_ID
	   ,S.HOSP_ADMSN_TIME
	   ,S.HOSP_DISCH_TIME
	   ,givmed.*
	   ,meddim.MedType
	   ,meddim.InProtocol
	   ,Acetaminophenct = CASE WHEN meddim.metricgrouper = 'Acetaminophen' THEN 1 ELSE 0 END
	   ,Celebrexct = CASE WHEN meddim.metricgrouper = 'Celebrex' THEN 1 ELSE 0 END
	   ,GabaLyrica = CASE WHEN meddim.metricgrouper = 'GabaLyrica' THEN 1 ELSE 0 END
	FROM
		#Source S
	JOIN #GivenMeds givmed ON CASE WHEN givmed.admissioncsn_flag = 1 THEN S.PAT_ENC_CSN_ID
								   WHEN givmed.anescsn_flag = 1 THEN S.Anes_Enc_Csn_ID
							  END = givmed.pat_enc_csn_id
	LEFT JOIN RADB.dbo.CRD_ERASOrtho_Med_Dim AS meddim ON givmed.MEDICATION_ID = meddim.medication_id
	WHERE
		meddim.MedType = 'Multi-Modal Analgesia'
		AND PreOp = 1
	),
	multifin AS(
	SELECT
		multibase.pat_enc_csn_id
	   ,SUM(Acetaminophenct) AS acetct
	   ,SUM(Celebrexct) AS celbrexct
	   ,SUM(GabaLyrica) AS gabalyricact
	   ,MAX(multibase.admissioncsn_flag) 'admissioncsn_flag'
	   ,MAX(multibase.anescsn_flag) 'anescsn_flag'
	   ,1 'MultiModal'
	FROM
		multibase
	GROUP BY
		multibase.pat_enc_csn_id
	) 

	UPDATE S
	SET MultiModal_Pain_Med = ISNULL(x.MultiModal,0)
	FROM
	 radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics S
	 LEFT JOIN 
	 (
		SELECT 
		s.PAT_ENC_CSN_ID
		,fin.MultiModal
		FROM radb.dbo.CRD_ERAS_YNHOBGYN_31893_ProcessMetrics S
		 JOIN #Encounters E ON E.PAT_ENC_CSN_ID = S.PAT_ENC_CSN_ID
		 JOIN  multifin AS fin ON CASE WHEN fin.admissioncsn_flag = 1 THEN E.PAT_ENC_CSN_ID
									   WHEN fin.anescsn_flag = 1 THEN E.Anes_Enc_Csn_ID
								  END = fin.pat_enc_csn_id
		WHERE fin.acetct>0 AND fin.gabalyricact>0 AND fin.celbrexct>0
	) x ON x.PAT_ENC_CSN_ID = s.PAT_ENC_CSN_ID



/*
		,Ambulate AS
		(
		SELECT 
		r.pat_enc_csn_id
	   ,t.LOG_ID
	   --,s.SURGERY_DATE
	   --,s.HOSP_DISCH_TIME
	  -- ,r.RECORDED_TIME
	   ,r.Ambulate_In_room
	   ,r.Ambulate_In_Hall
	   ,r.pt_bedtochair
	   ,r.pt_chairtobed
	   ,r.pt_sidesteps
	   ,r.pt_5ft
	   ,r.pt_10ft
	   ,r.pt_15ft
	   ,r.pt_20ft
	   ,r.pt_25ft
	   ,r.pt_50ft
	   ,r.pt_75ft
	   ,r.pt_100ft
	   ,r.pt_150ft
	   ,r.pt_200ft
	   ,r.pt_250ft
	   ,r.pt_300ft
	   ,r.pt_350ft
	   ,r.pt_400ft
	   ,r.pt_x2
	   ,r.pt_x3

	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PreOp_Start AND t.PreOp_End THEN 1 ELSE 0 END 'PreOp'
	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.IntraOp_Start AND t.IntraOp_End THEN 1 ELSE 0 END 'IntraOp'
	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PACU_In AND t.PACU_OUT THEN 1 ELSE 0 END 'PACU'
	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_Start AND t.PostOp_D1 THEN 1 ELSE 0 END 'PostOp_D0'
	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D1 AND t.PostOp_D2 THEN 1 ELSE 0 END 'PostOp_D1' 
	   ,CASE WHEN r.RECORDED_TIME BETWEEN t.PostOp_D2 AND CASE WHEN s.HOSP_DISCH_TIME < t.PostOp_D3 THEN s.HOSP_DISCH_TIME ELSE PostOp_D3 END THEN 1 ELSE 0 END 'PostOp_D2'
		--rolled.pat_enc_csn_id
		--,CASE WHEN rolled.Ambulate_In_room + rolled.Ambulate_In_Hall > 0 THEN 1 ELSE 0 End
		FROM rolled r
		JOIN #Source s ON s.PAT_ENC_CSN_ID = r.pat_enc_csn_id
		JOIN radb.dbo.CRD_ERAS_Global_TimeWindows_vw t ON t.LOG_ID = s.LOG_ID
		)
		--SELECT * FROM Ambulate

		UPDATE pm
		SET 

		pm.PostOp_D0_Ambulation=isnull(a.PostOp_D0_Ambulation, 0)

		,pm.PostOp_D1_Ambulation=isnull(FL.PostOp_D1_Ambulation, 0)

		,pm.PostOp_D2_Ambulation=isnull(FL.PostOp_D2_Ambulation, 0)

		FROM RADB.[dbo].[CRD_ERAS_YNHOBGYN_31893_ProcessMetrics] pm
		JOIN 
		(
		SELECT
		 am.pat_enc_csn_id
		,am.LOG_ID
		,CASE WHEN am.PostOp_D0 = 1 
					AND (am.Ambulate_In_room + 	am.Ambulate_In_Hall + 	am.pt_bedtochair + 	am.pt_chairtobed + 	am.pt_sidesteps + 	am.pt_5ft + 	am.pt_10ft + 	am.pt_15ft + 	am.pt_20ft + 	am.pt_25ft + 	am.pt_50ft + 	am.pt_75ft + 	am.pt_100ft + 	am.pt_150ft + 	am.pt_200ft + 	am.pt_250ft + 	am.pt_300ft + 	am.pt_350ft + 	am.pt_400ft + 	am.pt_x2 + 	am.pt_x3  ) 
					> 1 THEN 1 ELSE 0 END 'PostOp_D0_Ambulation'

		From
		Ambulate am
		) a ON a.LOG_ID = pm.LOG_ID

*/



/*
--Cath example code
								SELECT  p.PAT_NAME
								,       p.PAT_MRN_ID
								--,       f.hosp_admsn_time
								--,       f.surgery_date
								--,       f.sched_start_time
								--,       f.pod0_start
								--,       f.pod1_start
								--,       f.pod2_start
								,       iln.PAT_ENC_CSN_ID
								,       iln.IP_LDA_ID
								,       iln.PLACEMENT_INSTANT
								--,		CASE WHEN iln.REMOVAL_INSTANT>=pod0_start AND iln.REMOVAL_INSTANT<pod2_start THEN 1 ELSE 0 END AS removalflag
								,       iln.REMOVAL_INSTANT
								,       iln.DESCRIPTION
								,       iln.PROPERTIES_DISPLAY
								,		iln.FSD_ID
								,		ifgd.DUPLICATEABLE_YN
								,		ifgd.FLO_MEAS_NAME
								,		ifgd.DISP_NAME
								,		rowtype=zrt.name
								FROM   clarity.dbo.IP_DATA_STORE AS ids

										JOIN clarity.dbo.IP_LDA_INPS_USED AS iliu
										ON ids.INPATIENT_DATA_ID=iliu.INP_ID
										JOIN clarity.dbo.IP_LDA_NOADDSINGLE AS iln 
												ON iln.IP_LDA_ID=iliu.IP_LDA_ID				        
										LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd
										ON iln.FLO_MEAS_ID=ifgd.FLO_MEAS_ID				
										LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt
										ON ifgd.ROW_TYP_C=zrt.ROW_TYP_C
									   LEFT JOIN clarity.dbo.pat_enc_hsp AS peh ON ids.EPT_CSN = peh.PAT_ENC_CSN_ID
									   JOIN clarity.dbo.PATIENT AS p ON peh.pat_id = p.PAT_ID
								WHERE   iln.FLO_MEAS_ID IN ( '8148', '8151' /*'3048148000'*/ )
								AND ids.EPT_CSN = '128461755'
*/


--dashboard feed
sp_helptext CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw


CREATE VIEW [dbo].[CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw]
AS



--SELECT * FROM dbo.CRD_ERAS_YNHOBGYN_31893_DashboardFeed_vw
--WHERE id = 17
----AND pat_ENC_CSN_ID = '112318601'



SELECT
/*Metric Dim*/
md.ID
,md.MetricShort
,md.MetricName
,md.MetricCalculation
,md.TrendOrd
,md.metricgroup

/*Metric Date*/
,mDate.full_date
,mDate.Day
,mDate.day_of_week
,mDate.day_num_in_month
,mDate.day_num_overall
,mDate.day_name
,mDate.day_abbrev
,mDate.weekday_flag
,mDate.week_num_in_year
,mDate.week_num_overall
,mDate.week_begin_date
,mDate.week_begin_date_key
,mDate.month
,mDate.month_num_overall
,mDate.month_name
,mDate.month_abbrev
,mDate.quarter
,mDate.year
,mDate.yearmo
,mDate.fiscal_month
,mDate.fiscal_quarter
,mDate.fiscal_year
,mDate.last_day_in_month_flag
,mDate.same_day_year_ago_date
,mDate.week_end_sat_date
,mDate.week_end_sat_date_key
,mDate.First_of_Month
,mDate.week_end_sun_date
,mDate.week_begin_date_sun

/*Metric Fact*/
,MF.PAT_ENC_CSN_ID
,MF.IDGroup2
,MF.IdGroup2Type
,MF.IDGroup3
,MF.IdGroup3Type
,MF.PAT_MRN_ID
,MF.DateKey
,MF.Num
,MF.Den
,MF.RptGroup1

/*Provider Information*/
,ser.PROV_NAME
FROM RADB.dbo.CRD_ERAS_YNHOBGYN_31893_MetricDim md
JOIN [RADB].[dbo].[CRD_ERAS_YNHOBGYN_31893_DateDim] mDate ON md.ID = mDate.metId
LEFT JOIN RADB.dbo.CRD_ERAS_YNHOBGYN_31893_MetricFact MF ON mDate.metid = mf.MetricKey AND mDate.date_key = mf.DateKey
/*Provider Information*/
LEFT JOIN clarity.dbo.CLARITY_SER ser ON ser.PROV_ID = mf.IDGroup3





