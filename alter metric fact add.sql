




USE [RADB]
GO

/****** Object:  View [dbo].[CRD_ERAS_GHGI_MetricFact]    Script Date: 8/26/2016 4:10:42 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

alter VIEW [dbo].[CRD_ERAS_GHGI_MetricFact]
AS

SELECT --median los
		CAST('1' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter AS ERASRptGrouper
	   ,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,Discharge_DateKey AS DateKey
	   ,LOSDays AS Num
	   ,1 'Den'
	FROM
		radb.dbo.vw_CRD_ERAS_GHGI_EncDim
UNION ALL

SELECT --# encounters
		CAST('38' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   , NULL AS Log_ID
	   ,ERASEncounter
	   ,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.Discharge_DateKey
	   ,1 AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_GHGI_EncDim AS  ah

UNION ALL

SELECT --average los
		CAST('2' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter
	   ,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,Discharge_DateKey
	   ,LOSDays AS Num
	   ,1 'Den'
	FROM
		radb.dbo.vw_CRD_ERAS_GHGI_EncDim

UNION ALL

SELECT --median los proc to disch
		CAST('62' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter AS ERASRptGrouper
	   ,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,Discharge_DateKey AS DateKey
	   ,proclos AS Num
	   ,1 'Den'
	FROM	(
		SELECT  e.csn, e.Discharge_DateKey,DATEDIFF(DAY,c.procedurefinish, e.Discharge_DTTM) AS proclos,e.ERASEncounter,OrdersetFlag
		FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS c
		LEFT JOIN radb.dbo.vw_CRD_ERAS_GHGI_EncDim e ON c.AdmissionCSN=e.CSN
		) AS c

		
UNION ALL

SELECT --avg los proc to disch
		CAST('61' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter AS ERASRptGrouper
	   ,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,Discharge_DateKey AS DateKey
	   ,proclos AS Num
	   ,1 'Den'
	FROM	(
		SELECT  e.csn, e.Discharge_DateKey,DATEDIFF(DAY,c.procedurefinish, e.Discharge_DTTM) AS proclos,e.ERASEncounter,e.OrdersetFlag
		FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS c
		LEFT JOIN radb.dbo.vw_CRD_ERAS_GHGI_EncDim e ON c.AdmissionCSN=e.CSN
		) AS c


UNION ALL



SELECT --readmission rate
		CAST('3' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter
	   	,CASE WHEN OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.Discharge_DateKey
	   ,ah.HospitalWide_30DayReadmission_NUM AS Num
	   ,ah.HospitalWide_30DayReadmission_DEN AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_GHGI_EncDim ah		


UNION ALL

SELECT --% liquids 2 hrs before induction
		CAST('50' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_3ind AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 LEFT JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
		 

UNION ALL

SELECT --3.2 % open cases with lumbar epidural
		CAST('56' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.lumbar_epi AS Num
	   ,1 Den
	FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
		 WHERE ah.ProcedureType='Open'


UNION ALL

SELECT --7.1 % normal PACU temperature
		CAST('51' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.NormalTempInPacu AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		  JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
UNION ALL 

SELECT --% 9.1 PO liquids POD 0
		CAST('57' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_pod0 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN

UNION ALL

SELECT --% 9.2 PO liquids POD 1
		CAST('58' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_pod1 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
UNION ALL

SELECT --% 9.3 Solid foods POD 2
		CAST('60' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.solidfood_pod2  AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
UNION ALL


SELECT --10.1 % ambulate day 0
		CAST('52' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.ambulatepod0,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.ambulatepod0 AS Num
	   ,CASE WHEN CONVERT(time,outofpacu)<'18:00'  THEN  1  ELSE 0 END AS Den	   
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN
UNION ALL

SELECT --10.2 ambulate pod 1
		CAST('53' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod1 AS Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN

UNION ALL

SELECT --10.3 ambulate pod 2
		CAST('54' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod2 AS Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN

UNION ALL


SELECT --12.1 % return of bowel function
		CAST('59' AS INT) AS 'MetricKey'
	   ,ISNULL(AdmissionCSN,NULL) 'PAT_ENC_CSN_ID'
	   , Log_ID
	   ,ErasCase
	   ,CASE WHEN enc.OrdersetFlag=1 THEN 'Yes' ELSE 'No' END AS Orderset
	   ,SurgeryDateKey
	   ,CASE WHEN hrs_tobowelfunction IS NOT NULL THEN 1 ELSE 0 END
	   ,1 AS Den
	FROM 
		radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS enc ON enc.PAT_ENC_CSN_ID=ah.AdmissionCSN


GO


