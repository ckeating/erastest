--master join for report

EXEC radb.dbo.CRD_ERAS_Create_DateDim


SELECT * FROM  dbo.CRD_ERAS_MetricDim AS cemd
WHERE MetricName LIKE '#%'


ROLLBACK
BEGIN tran
COMMIT
delete dbo.CRD_ERAS_MetricDim 
WHERE MetricName LIKE '#%'




SELECT *
FROM dbo.vw_CRD_ERAS_EncDim 
WHERE Discharge_DTTM>'5/20/2016'
AND HospitalWide_30DayReadmission_DEN=1



SELECT * FROM radb.dbo.vw_CRD_ERAS_Report

ALTER VIEW dbo.vw_CRD_ERAS_Report
as
SELECT m.MetricName
,m.MetricNumber
,m.MetricDefinition
,m.MetricCalculation
,m.MetricType
,m.TrendOrd
,dt.full_date
,dt.week_begin_date
,f.ERASRptGrouper
,f.Num
,f.Den
,c.Log_ID ,
 c.ProcedureType ,
 c.Surgery_Patient_Class ,
 c.OrLog_Status_C ,
 c.LogStatus ,
 c.CASE_CLASS_C ,
 c.CASECLASS_DESCR ,
 c.NUM_OF_PANELS ,
 c.ProcedureDisplayName ,
 c.ErasCase ,
 c.CPT_Code ,
 c.AnesCSN ,
 c.AdmissionCSN ,
 c.SurgicalCSN ,
 c.ProcedureName ,
 c.Surgery_Room_Name ,
 c.SurgeonName ,
 c.Surgeon_Role_C ,
 c.Panel ,
 c.ALL_PROCS_PANEL ,
 c.procline ,
 c.SurgeryServiceName ,
 c.SurgeryDate ,
  c.Sched_Start_Time ,
 c.SurgeryLocation ,
 c.setupstart ,
 c.setupend ,
 c.inroom ,
 c.outofroom ,
 c.cleanupstart ,
 c.cleanupend ,
 c.inpacu ,
 c.outofpacu ,
 c.inpreprocedure ,
 c.outofpreprocedure ,
 c.anesstart ,
 c.anesfinish ,
 c.procedurestart ,
 c.procedurefinish ,
 c.postopday1_begin ,
 c.postopday2_begin ,
 c.postopday3_begin ,
 c.postopday4_begin ,
 c.CaseLength_min ,
 c.CaseLength_hrs ,
 c.timeinpacu_min ,
 c.pacudelay ,
 c.preadm_counseling ,
 c.[Received pre admission counseling?] ,
 c.TemperatureInPacu ,
 c.[Normal temp on arrival to PACU?] ,
 c.NormalTempInPacu ,
 c.[Ambulate POD0?] ,
 c.ambulatepod0 ,
 c.clearliquids_3ind ,
 c.[Clear liq 3 hrs before induction?] ,
 c.clearliquids_pod0 ,
 c.[Clear liq given POD0?] ,
 c.ambulate_pod1 ,
 c.[Ambulate POD1?] ,
 c.solidfood_pod1 ,
 c.[Solid food POD1?] ,
 c.ambulate_pod2 ,
 c.[Ambulate POD2?] ,
 c.hrs_toleratediet,
 e.CSN ,
 e.HAR ,
 e.PatientName,
 e.MRN,	
 e.LOSDays ,
 e.LOSHours ,
 e.Admission_DTTM ,
 e.Admission_DT ,
 e.Discharge_DTTM ,
 e.Discharge_DT ,
 e.Discharge_DateKey ,
 e.Enc_DischargeDisposition ,
 e.PatientStatus ,
 e.BaseClass ,
 e.Enc_Pat_Class ,
 e.[Admission Type] ,
 e.HospitalWide_30DayReadmission_NUM ,
 e.HospitalWide_30DayReadmission_DEN ,
 e.NumberofProcs ,
 e.qvi_Infection ,
 e.qvi_AdverseEffects ,
 e.qvi_FallsTrauma ,
 e.qvi_ForeignObjectRetained ,
 e.qvi_PerforationLaceration ,
 e.qvi_DVTPTE ,
 e.qvi_Pneumonia ,
 e.qvi_Shock ,
 e.qvi_Any
FROM radb.dbo.CRD_ERAS_MetricDim AS m
JOIN radb.dbo.CRD_ERAS_MetDate AS dt ON m.id=dt.MetID
LEFT JOIN radb.dbo.CRD_ERAS_MetricFact AS f ON f.DateKey=dt.date_key
											AND f.MetricKey=dt.MetID
LEFT JOIN radb.dbo.vw_CRD_ERAS_Case AS c ON c.LOG_ID=f.Log_ID
LEFT JOIN radb.dbo.vw_CRD_ERAS_EncDim AS e ON e.CSN=f.PAT_ENC_CSN_ID


SELECT * FROM dbo.vw_CRD_ERAS_Report

SELECT * FROM dbo.vw_CRD_ERAS_Report_Detail

CREATE VIEW dbo.vw_CRD_ERAS_Report_Detail
as
SELECT  e.CSN ,
        e.HAR ,
        e.PatientName ,
        e.MRN ,
        e.Admission_DTTM ,
        e.Discharge_DTTM ,
        e.LOSDays ,
        e.LOSHours ,
        e.Enc_DischargeDisposition ,
        e.PatientStatus ,
        e.BaseClass ,
        e.Enc_Pat_Class ,
        e.[Admission Type] ,
        c.Log_ID ,
        c.ProcedureDisplayName ,
        c.ErasCase ,
        c.CPT_Code ,
        c.SurgeryDate ,
        c.ProcedureType ,
        c.Surgery_Patient_Class ,
        c.LogStatus ,
        c.ProcedureName ,
        c.SurgeonName ,
		c.SurgeonRole ,
        [Surg Log Class] = ISNULL(c.CASECLASS_DESCR,
                                  '*Unknown surgical log class') ,
        c.NUM_OF_PANELS AS NumOfPanels ,
        c.AnesCSN ,
        c.AdmissionCSN ,
        c.SurgicalCSN ,
        c.Surgery_Room_Name ,        
        c.ALL_PROCS_PANEL ,
        c.procline ,
        c.SurgeryServiceName ,
        c.Sched_Start_Time ,
        c.SurgeryLocation ,
        c.setupstart ,
        c.setupend ,
        c.inroom ,
        c.outofroom ,
        c.cleanupstart ,
        c.cleanupend ,
        c.inpacu ,
        c.outofpacu ,
        c.inpreprocedure ,
        c.outofpreprocedure ,
        c.anesstart ,
        c.anesfinish ,
        c.procedurestart ,
        c.procedurefinish ,
        c.postopday1_begin ,
        c.postopday2_begin ,
        c.postopday3_begin ,
        c.postopday4_begin ,
        c.CaseLength_min ,
        c.CaseLength_hrs ,
        c.timeinpacu_min ,        
        c.preadm_counseling ,
        c.[Received pre admission counseling?] ,
        c.TemperatureInPacu ,
        c.[Normal temp on arrival to PACU?] ,
        c.NormalTempInPacu ,
        c.[Ambulate POD0?] ,
        c.ambulatepod0 ,
        c.clearliquids_3ind ,
        c.[Clear liq 3 hrs before induction?] ,
        c.clearliquids_pod0 ,
        c.[Clear liq given POD0?] ,
        c.ambulate_pod1 ,
        c.[Ambulate POD1?] ,
        c.solidfood_pod1 ,
        c.[Solid food POD1?] ,
        c.ambulate_pod2 ,
        c.[Ambulate POD2?] ,
        c.hrs_toleratediet ,
        e.Admission_DT ,
        e.Discharge_DT ,
        e.HospitalWide_30DayReadmission_NUM ,
        e.HospitalWide_30DayReadmission_DEN ,
        e.NumberofProcs ,
		e.TotalDirectCost,
        e.qvi_Infection ,
        e.qvi_AdverseEffects ,
        e.qvi_FallsTrauma ,
        e.qvi_ForeignObjectRetained ,
        e.qvi_PerforationLaceration ,
        e.qvi_DVTPTE ,
        e.qvi_Pneumonia ,
        e.qvi_Shock ,
        e.qvi_Any
FROM    RADB.dbo.vw_CRD_ERAS_EncDim AS e
        LEFT JOIN RADB.dbo.vw_CRD_ERAS_Case AS c ON c.AdmissionCSN = e.CSN



SELECT Log_ID ,
       ProcedureType ,
       PatientName ,
       MRN ,
       Surgery_Patient_Class ,
       OrLog_Status_C ,
       LogStatus ,
       CASE_CLASS_C ,
       CASECLASS_DESCR AS Case,
       NUM_OF_PANELS ,
       ProcedureDisplayName ,
       ErasCase ,
       CPT_Code ,
       AnesCSN ,
       AdmissionCSN ,
       SurgicalCSN ,
       ProcedureName ,
       Surgery_Room_Name ,
       SurgeonName ,
       SurgeonRole=ISNULL(or_role.name,'*Unknown surgeon role'),
       Panel ,
       ALL_PROCS_PANEL ,
       procline ,
       SurgeryServiceName ,
       SurgeryDate ,
       SurgeryDateKey ,
       Sched_Start_Time ,
       SurgeryLocation ,
       setupstart ,
       setupend ,
       inroom ,
       outofroom ,
       cleanupstart ,
       cleanupend ,
       inpacu ,
       outofpacu ,
       inpreprocedure ,
       outofpreprocedure ,
       anesstart ,
       anesfinish ,
       procedurestart ,
       procedurefinish ,
       postopday1_begin ,
       postopday2_begin ,
       postopday3_begin ,
       postopday4_begin ,
       CaseLength_min ,
       CaseLength_hrs ,
       timeinpacu_min ,
       pacudelay ,
       preadm_counseling ,
       [Received pre admission counseling?] ,
       TemperatureInPacu ,
       [Normal temp on arrival to PACU?] ,
       NormalTempInPacu ,
       [Ambulate POD0?] ,
       ambulatepod0 ,
       clearliquids_3ind ,
       [Clear liq 3 hrs before induction?] ,
       clearliquids_pod0 ,
       [Clear liq given POD0?] ,
       ambulate_pod1 ,
       [Ambulate POD1?] ,
       solidfood_pod1 ,
       [Solid food POD1?] ,
       ambulate_pod2 ,
       [Ambulate POD2?] ,
       hrs_toleratediet 
FROM radb.dbo.vw_CRD_ERAS_Case AS c
LEFT JOIN clarity.dbo.ZC_OR_PANEL_ROLE AS or_role ON c.Surgeon_Role_C=or_role.ROLE_C






SELECT * 
FROM dbo.clarity_dep
WHERE DEPARTMENT_NAME LIKE '%sicu%'


SELECT * FROM dbo.CRD_ERASOrtho_EncDim_vw  

SELECT * 
from radb.dbo.CRD_ERAS_MetricDim 
WHERE MetricType='process'


SELECT * FROM dbo.vw_CRD_ERAS_Case


alter VIEW dbo.vw_CRD_ERAS_Case
AS
SELECT LOG_ID AS Log_ID,
       ProcedureType ,
       PAT_NAME AS PatientName,
       PAT_MRN_ID AS MRN,              
       Surgery_Patient_Class ,
       STATUS_C AS OrLog_Status_C,
       LogStatus ,
       CASE_CLASS_C ,
       CASECLASS_DESCR ,
       NUM_OF_PANELS ,
       PROC_DISPLAY_NAME AS ProcedureDisplayName,
       ErasCase ,
       REAL_CPT_CODE AS CPT_Code,
       anescsn AS AnesCSN,
       admissioncsn AS AdmissionCSN,
       surgicalcsn AS SurgicalCSN,
       procedurename AS ProcedureName,
       Surgery_Room_Name ,
       SurgeonName ,
       ROLE_C AS Surgeon_Role_C,
       PANEL AS Panel,
       ALL_PROCS_PANEL ,
       procline ,
       SurgeryServiceName ,
       SURGERY_DATE AS SurgeryDate,
       DateKey AS SurgeryDateKey,
       SCHED_START_TIME AS Sched_Start_Time,
       SurgeryLocation ,
       setupstart ,
       setupend ,
       inroom ,
       outofroom ,
       cleanupstart ,
       cleanupend ,
       inpacu ,
       outofpacu ,
       inpreprocedure ,
       outofpreprocedure ,
       anesstart ,
       anesfinish ,
       procedurestart ,
       procedurefinish ,
       postopday1_begin ,
       postopday2_begin ,
       postopday3_begin ,
       postopday4_begin ,
       CaseLength_min ,
       CaseLength_hrs ,
       timeinpacu_min ,
       pacudelay ,
       preadm_counseling ,
	   [Received pre admission counseling?]=CASE WHEN preadm_counseling=1 THEN 'Yes' ELSE 'No' END,
       pacutemp AS TemperatureInPacu,
	   [Normal temp on arrival to PACU?]=CASE WHEN NormalTempInPacu=1 THEN 'Yes' ELSE 'No' END,
       NormalTempInPacu ,
	   [Ambulate POD0?]=CASE WHEN ambulatepod0=1 THEN 'Yes' ELSE 'No' END,
       ambulatepod0 ,
       clearliquids_3ind ,
	   [Clear liq 3 hrs before induction?]=CASE WHEN clearliquids_3ind=1 THEN 'Yes' ELSE 'No'END,
       clearliquids_pod0 ,
	   [Clear liq given POD0?]=CASE WHEN clearliquids_pod0=1 THEN 'Yes' ELSE 'No' END,
       ambulate_pod1 ,
	   [Ambulate POD1?]=CASE WHEN ambulate_pod1=1 THEN 'Yes' ELSE 'No' END,
       solidfood_pod1 ,
	   [Solid food POD1?]=CASE WHEN solidfood_pod1=1 THEN 'Yes' ELSE 'No' END,
       ambulate_pod2 ,
	   [Ambulate POD2?]=CASE WHEN ambulate_pod2=1 THEN 'Yes' ELSE 'No' END,
       hrs_toleratediet 

FROM radb.dbo.CRD_ERAS_Case AS cec



create VIEW dbo.vw_CRD_ERAS_EncDim 
AS
SELECT PAT_ENC_CSN_ID AS CSN,
       HSP_ACCOUNT_ID AS HAR,
       LOSDays ,
       LOSHours ,
       HOSP_ADMSN_TIME AS Admission_DTTM,
	   CONVERT(DATE,HOSP_ADMSN_TIME) AS Admission_DT,
       HOSP_DISCH_TIME AS Discharge_DTTM,
	   CONVERT(DATE,HOSP_DISCH_TIME) AS Discharge_DT,
       Discharge_DateKey ,       
       Enc_DischargeDisposition ,       
       PatientStatus ,              
       BaseClass ,
       Enc_Pat_Class ,       
       [Admission Type] ,
       HospitalWide_30DayReadmission_NUM ,
       HospitalWide_30DayReadmission_DEN ,
       NumberofProcs 
FROM radb.dbo.CRD_ERAS_EncDim AS ceed

SELECT * FROM dbo.vw_CRD_ERAS_EncDim 


IF OBJECT_ID('radb.dbo.CRD_ERAS_MetricDim') IS NOT NULL
	DROP TABLE radb.dbo.CRD_ERAS_MetricDim;

CREATE TABLE [dbo].[CRD_ERAS_MetricDim](
	[ID] [INT] NOT NULL IDENTITY (1,1),
	MetricNumber INT,
	[MetricName] [VARCHAR](500) NULL,
	[MetricDefinition] [VARCHAR](1000) NULL,
	[MetricCalculation] [VARCHAR](250) NULL,
	[MetricType] [VARCHAR](250) NULL,
	[TrendOrd] [INT] NULL,
	[Grain] [VARCHAR](250) NULL,
	[Numerator] [VARCHAR](250) NULL,
	[Denominator] [VARCHAR](250) NULL,
	[InProtocol] [VARCHAR](250) NULL
) ON [PRIMARY]


INSERT radb.dbo.CRD_ERAS_MetricDim
        ( MetricNumber,
		  MetricName ,
          MetricDefinition ,
          MetricCalculation ,
          MetricType ,
          TrendOrd ,
          Grain ,
          Numerator ,
          Denominator ,
          InProtocol
        )
SELECT NULL,
	   MetricName ,
       MetricDefinition ,
       MetricCalculation ,
       MetricType ,
       TrendOrd ,
       Grain ,
       Numerator ,
       Denominator ,
       InProtocol
FROM radb.dbo.CRD_ERASOrtho_MetricDim

sp_help CRD_ERAS_MetricDim 
sp_help CRD_ERASOrtho_MetricDim 

FROM radb.dbo.CRD_ERASOrtho_MetricDim 







sp_helptext CRD_ERASOrtho_EncDim_vw




SELECT SUM(num)


SELECT met.MetricName
,	met.MetricDefinition
,met.MetricCalculation
,met.TrendOrd
,met.Grain
 md.full_date ,
 md.Day ,
 md.day_of_week ,
 md.day_num_in_month ,
 md.day_num_overall ,
 md.day_name ,
 md.day_abbrev ,
 md.weekday_flag ,
 md.week_num_in_year ,
 md.week_num_overall ,
 md.week_begin_date ,
 md.week_begin_date_key ,
 md.month ,
 md.month_num_overall ,
 md.month_name ,
 md.month_abbrev ,
 md.quarter ,
 md.year ,
 md.yearmo ,
 md.fiscal_month ,
 md.fiscal_quarter ,
 md.fiscal_year ,
 md.last_day_in_month_flag ,
 md.same_day_year_ago_date ,
 md.week_end_sat_date ,
 md.week_end_sat_date_key ,
 md.First_of_Month ,
 md.week_end_sun_date ,
 md.week_begin_date_sun,
 mf.Num,
 mf.Den,
 enc.

FROM radb.dbo.CRD_ERASOrtho_MetricDim AS met
JOIN radb.dbo.CRD_ERASOrtho_MetDate AS md ON met.ID=md.MetID 
LEFT JOIN radb.dbo.CRD_ERASOrtho_MetricFact AS mf ON mf.MetricKey=md.MetID AND mf.DateKey=md.date_key
LEFT JOIN radb.dbo.CRD_ERASOrtho_EncDim_vw AS enc ON enc.CSN=mf.PAT_ENC_CSN_ID
LEFT JOIN radb.dbo.CRD_ERASOrtho_Cases AS c ON c.PAT_ENC_CSN_ID=enc.csn
WHERE c.PAT_MRN_ID='mr38340'



SELECT *
FROM radb.dbo.CRD_ERASOrtho_EncDim_vw AS enc 
LEFT JOIN radb.dbo.CRD_ERASOrtho_Cases AS c ON c.PAT_ENC_CSN_ID=enc.csn
WHERE c.PAT_MRN_ID='mr531399'



SELECT * FROM radb.dbo.CRD_ERASOrtho_cases

EXEC radb.dbo.CRD_ERASOrtho_DateDim

sp_helptext CRD_ERASOrtho_CreateDateDim





SELECT *
--COUNT(DISTINCT pat_enc_csn_id)
INTO ##chi
from radb.dbo.CRD_ERASOrtho_MetricFact AS mf 
--LEFT JOIN radb.dbo.CRD_ERASOrtho_EncDim_vw AS enc ON enc.CSN=mf.PAT_ENC_CSN_ID
WHERE mf.MetricKey=38

WITH fixit AS (
SELECT	rid=ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY PAT_ENC_CSN_ID)
		,* 
FROM ##chi
)SELECT * 
FROM fixit 
WHERE fixit.PAT_ENC_CSN_ID IN (SELECT pat_enc_csn_id FROM fixit WHERE rid>1)


WITH fixit AS (
SELECT	rid=ROW_NUMBER() OVER (PARTITION BY csn ORDER BY csn)
		,* 
FROM radb.dbo.CRD_ERASOrtho_EncDim_vw
)SELECT * 
FROM fixit 
WHERE fixit.csn IN (SELECT csn FROM fixit WHERE rid>1)


WITH fixit AS (
SELECT	rid=ROW_NUMBER() OVER (PARTITION BY ceoc.PAT_ENC_CSN_ID ORDER BY ceoc.PAT_ENC_CSN_ID)
		,* 
FROM radb.dbo.CRD_ERASOrtho_Cases AS ceoc
)SELECT * 
FROM fixit 
WHERE fixit.PAT_ENC_CSN_ID IN (SELECT  PAT_ENC_CSN_ID  FROM fixit WHERE rid>1)





SELECT * 
FROM radb.dbo.CRD_ERASOrtho_MetricDim AS ceomd
WHERE me


SELECT --# encounters
		CAST('38' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   , NULL AS Log_ID
	   ,ah.DateKey
	   ,1 AS Num
	   ,1 AS Den
	FROM
		radb.dbo.CRD_ERASOrtho_EncDim_vw AS  ah


SELECT [Discharge Disposition],COUNT(*)
FROM radb.dbo.CRD_ERASOrtho_EncDim_vw 
GROUP BY [Discharge Disposition]

SELECT * 
FROM dbo.CRD_ERASOrtho_Cases AS ceoc
WHERE ceoc.PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM dbo.CRD_ERASOrtho_Cases GROUP BY PAT_ENC_CSN_ID HAVING COUNT(*)>1)


SELECT COUNT(*)
FROM radb.dbo.CRD_ERASOrtho_Cases AS ceoc

BEGIN tran
UPDATE dbo.CRD_ERASOrtho_MetricDim 
SET MetricCalculation='Average' WHERE id=22
SET trendord=CASE WHEN TrendOrd=-1 THEN 0 
				WHEN TrendOrd=1 THEN 1 END
ROLLBACK
				  
COMMIT

SELECT * FROM RADB.dbo.CRD_Asthma_MetDate [dbo].[CRD_Asthma_MetricDim]

USE [RADB]
GO

/****** Object:  Table [dbo].[CRD_Asthma_MetricDim]    Script Date: 1/25/2016 2:58:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[CRD_ERASOrtho_MetricDim](
	[ID] [INT]  NOT NULL,
	[MetricName] [VARCHAR](500) NULL,
	MetricDefinition VARCHAR(1000) NULL,
	[MetricCalculation] [VARCHAR](250) NULL,
	MetricType VARCHAR(250) NULL,
	[TrendOrd] [INT] NULL,
	Grain VARCHAR(250) NULL,
	Numerator VARCHAR(250) NULL,
	Denominator varchar(250) NULL,
    InProtocol varchar(250) null
) ON [PRIMARY]

GO


SELECT * FROM radb.dbo.CRD_ERASOrtho_MetricDim

TRUNCATE TABLE radb.dbo.CRD_ERASOrtho_MetricDim

SET ANSI_PADDING OFF
GO
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (1,'Median Length of stay','Median Length of stay','Median','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (2,'Average Length of stay','Average length of stay','Average','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (3,'30 day Hospital wide readmission rate','The 30 day hospital wide readmission rate','Ratio','Outcome','-1','Encounter','Total number of inpatient discharges who are eligible for readmission and who  have all-cause unplanned readmissions within 30-days of discharge.  ','Total number of inpatient discharges eligible for readmission','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (4,'# QVI – PE/DVT','The number of encounters with a QVI PE/DVT event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (5,'% QVI – PE/DVT','The percentage of encounters with a QVI PE/DVT event.','Ratio','Outcome','-1','Encounter','QVI-PE/DVT event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (6,'# QVI – Adverse Events','The number of encounters with a QVI Adverse Event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (7,'% QVI – Adverse Events','The percentage of total encounters with a QVI Adverse event','Ratio','Outcome','-1','Encounter','QVI-Adverse event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (8,'# Any QVI events','The number of encounters with any QVI event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (9,'% Any QVI events','The percentage of total encounters with any QVI  event','Ratio','Outcome','-1','Encounter','Any QVI event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (10,'# QVI – Falls Trauma','The number of encounters with a QVI Falls Trauma event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (11,'% QVI – Falls Trauma','The percentage of total encounters with a QVI Falls Trauma event','Ratio','Outcome','-1','Encounter','QVI Falls Trauma event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (12,'# QVI – Foreign Object Retained','The number of encounters with a QVI Foreign Object Retained event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (13,'% QVI – Foreign Object Retained','The percentage of total encounters with a QVI Foreign Object Retained event','Ratio','Outcome','-1','Encounter','QVI-Foreign Object Retained event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (14,'# QVI – Infection','The number of encounters with a QVI Infection event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (15,'% QVI – Infection','The percentage of total encounters with a QVI Infection event','Ratio','Outcome','-1','Encounter','QVI-Infection event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (16,'# QVI – Perforation Laceration','The number of encounters with a QVI Perforation Laceration event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (17,'% QVI – Perforation Laceration','The percentage of total encounters with a QVI Perforation Laceration event','Ratio','Outcome','-1','Encounter','QVI - Perforation Laceration event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (18,'# QVI – Pneumonia','The number of encounters with a QVI Pneumonia  event.','Sum','Outcome','-1','Encounter','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (19,'% QVI – Pneumonia','The percentage of total encounters with a QVI Pneumonia event','Ratio','Outcome','-1','Encounter','QVI Pneumonia event','Total unique encounters','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (20,'# Ambulate post op day 0','Total cases where patients are ambulated on post op day 0 ','Sum','Process','1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (21,'% Ambulate post op day 0','Total percent of cases ambulated on post op day 0 ','Ratio','Process','1','Case','Total cases patients ambulated post op day 0','Total unique cases','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (22,'Avg Transfer to Floor Delay','Time from floor hold to procedure care complete if patient time in PACU is greater than 90 minutes  ','Avg','Process','-1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (23,'# Pre-op multi modal','Total number of cases multi-modal medications are administered pre-op','Sum','Process','1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (24,'% Pre-op multi modal','Total percent of cases multi-modal medications are administred pre-op','Ratio','Process','1','Case','Total number of cases multi-modal medications are administered pre-op','Total unique cases','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (25,'# Intra-op spinal anesthesia','# of cases where spinal anesthesia administered intra-op','Sum','Process','1','Case','','','Yes')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (26,'% Intra-op spinal anesthesia','% of cases where spinal anesthesia administered intra-op','Ratio','Process','1','Case','Total cases spinal anesthesia administered','Total unique cases','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (27,'# Intra-op Intra-articular injections','Total number of cases an intra-articular injection occurred intra-op.','Sum','Process','1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (28,'% Intra-op Intra-articular injections','Total percent of cases an intra-articular injection occurred intra-op.','Ratio','Process','1','Case','Total number of cases an intra-articular injection occurred intra-op.','Total unique cases','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (29,'# Intra-op departure from protocol','# of cases either Morphine (ERX 77009) or Bupivacaine (ERX 166538) are administered intra-op.','Sum','Process','-1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (30,'% Intra-op departure from protocol','Total percent of  cases either Morphine (ERX 77009) or Bupivacaine (ERX 166538) are administered intra-op.','Ratio','Process','-1','Case','# Intra-op departure from protocol','Total unique cases','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (31,'# Post-op pain management-parenteral','Total number of cases where IV narcotics are administered post-op. ','Sum','Process','-1','Case','','','No')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (32,'% Post-op pain management-parenteral','Total percent of cases where IV narcotics are administered post-op','Ratio','Process','-1','Case','# Post op IV narcotics administered','Total unique cases','No')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (33,'# Anti-emetics post op','Total number of cases where anti-emetics are administered post-op. This is a departure from protocol, due to the specific anesthesia used','Sum','Process','-1','Case','','','No')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (34,'% Anti-emetics post op','Percent of cases where anti-emetics are administered post-op.','Ratio','Process','-1','Case','# anti emetics administered post-op','Total unique cases','no')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (35,'# Foley catheter utilization','# of cases a straight cath documented post-op','Sum','Process','1','Case','','','')
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (36,'% Foley catheter utilization','Percent of cases where a straight cati  is administered post-op.','Ratio','Process','1','Case','# straight cath documented','Total unique cases','')


INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (1,'Median Length of stay','Median Length of stay','Median','Outcome','-1','Encounter','','','')

INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (2,'Average Length of stay','Average length of stay','Average','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (3,'30 day Hospital wide readmission rate','The 30 day hospital wide readmission rate','Ratio','Outcome','-1','Encounter','Total number of inpatient discharges who are eligible for readmission and who  have all-cause unplanned readmissions within 30-days of discharge.  ','Total number of inpatient discharges eligible for readmission','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (4,'# QVI – PE/DVT','The number of encounters with a QVI PE/DVT event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (5,'% QVI – PE/DVT','The percentage of encounters with a QVI PE/DVT event.','Ratio','Outcome','-1','Encounter','QVI-PE/DVT event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (6,'# QVI – Adverse Events','The number of encounters with a QVI Adverse Event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (7,'% QVI – Adverse Events','The percentage of total encounters with a QVI Adverse event','Ratio','Outcome','-1','Encounter','QVI-Adverse event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (8,'# Any QVI events','The number of encounters with any QVI event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (9,'% Any QVI events','The percentage of total encounters with any QVI  event','Ratio','Outcome','-1','Encounter','Any QVI event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (10,'# QVI – Falls Trauma','The number of encounters with a QVI Falls Trauma event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (11,'% QVI – Falls Trauma','The percentage of total encounters with a QVI Falls Trauma event','Ratio','Outcome','-1','Encounter','QVI Falls Trauma event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (12,'# QVI – Foreign Object Retained','The number of encounters with a QVI Foreign Object Retained event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (13,'% QVI – Foreign Object Retained','The percentage of total encounters with a QVI Foreign Object Retained event','Ratio','Outcome','-1','Encounter','QVI-Foreign Object Retained event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (14,'# QVI – Infection','The number of encounters with a QVI Infection event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (15,'% QVI – Infection','The percentage of total encounters with a QVI Infection event','Ratio','Outcome','-1','Encounter','QVI-Infection event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (16,'# QVI – Perforation Laceration','The number of encounters with a QVI Perforation Laceration event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (17,'% QVI – Perforation Laceration','The percentage of total encounters with a QVI Perforation Laceration event','Ratio','Outcome','-1','Encounter','QVI - Perforation Laceration event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (18,'# QVI – Pneumonia','The number of encounters with a QVI Pneumonia  event.','Sum','Outcome','-1','Encounter','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (19,'% QVI – Pneumonia','The percentage of total encounters with a QVI Pneumonia event','Ratio','Outcome','-1','Encounter','QVI Pneumonia event','Total unique encounters','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (20,'# Ambulate post op day 0','Total cases where patients are ambulated on post op day 0 ','Sum','Process','1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (21,'% Ambulate post op day 0','Total percent of cases ambulated on post op day 0 ','Ratio','Process','1','Case','Total cases patients ambulated post op day 0','Total unique cases','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (22,'Avg Transfer to Floor Delay','Time from floor hold to procedure care complete if patient time in PACU is greater than 90 minutes  ','Avg','Process','-1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (23,'# Pre-op multi modal','Total number of cases multi-modal medications are administered pre-op','Sum','Process','1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (24,'% Pre-op multi modal','Total percent of cases multi-modal medications are administred pre-op','Ratio','Process','1','Case','Total number of cases multi-modal medications are administered pre-op','Total unique cases','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (25,'# Intra-op spinal anesthesia','# of cases where spinal anesthesia administered intra-op','Sum','Process','1','Case','','','Yes',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (26,'% Intra-op spinal anesthesia','% of cases where spinal anesthesia administered intra-op','Ratio','Process','1','Case','Total cases spinal anesthesia administered','Total unique cases','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (27,'# Intra-op Intra-articular injections','Total number of cases an intra-articular injection occurred intra-op.','Sum','Process','1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (28,'% Intra-op Intra-articular injections','Total percent of cases an intra-articular injection occurred intra-op.','Ratio','Process','1','Case','Total number of cases an intra-articular injection occurred intra-op.','Total unique cases','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (29,'# Intra-op departure from protocol','# of cases either Morphine (ERX 77009) or Bupivacaine (ERX 166538) are administered intra-op.','Sum','Process','-1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (30,'% Intra-op departure from protocol','Total percent of  cases either Morphine (ERX 77009) or Bupivacaine (ERX 166538) are administered intra-op.','Ratio','Process','-1','Case','# Intra-op departure from protocol','Total unique cases','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (31,'# Post-op pain management-parenteral','Total number of cases where IV narcotics are administered post-op. ','Sum','Process','-1','Case','','','No',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (32,'% Post-op pain management-parenteral','Total percent of cases where IV narcotics are administered post-op','Ratio','Process','-1','Case','# Post op IV narcotics administered','Total unique cases','No',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (33,'# Anti-emetics post op','Total number of cases where anti-emetics are administered post-op. This is a departure from protocol, due to the specific anesthesia used','Sum','Process','-1','Case','','','No',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (34,'% Anti-emetics post op','Percent of cases where anti-emetics are administered post-op.','Ratio','Process','-1','Case','# anti emetics administered post-op','Total unique cases','no',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (35,'# Foley catheter utilization','# of cases a straight cath documented post-op','Sum','Process','1','Case','','','',
INSERT INTO radb.dbo.CRD_ERASOrtho_MetricDim VALUES (36,'% Foley catheter utilization','Percent of cases where a straight cati  is administered post-op.','Ratio','Process','1','Case','# straight cath documented','Total unique cases','',



SELECT * FROM  radb.dbo.CRD_ERASOrtho_MetricDim

BEGIN tran
UPDATE radb.dbo.CRD_ERASOrtho_MetricDim
SET MetricDefinition='Total number of encounters'
WHERE id=38

COMMIT
sp_helptext CRD_Asthma_Build_DateDim

DECLARE @RC INT

/*******************************************************************

Rebuild the date table to incorporate any new metrics

*******************************************************************/

SELECT 'Building Date Dimension'
EXECUTE @RC = RADB.dbo.CRD_Asthma_Build_DateDim 
SELECT 'Complete - Building Date Dimension ' + CAST(@@ROWCOUNT AS VARCHAR(100)) + ' With RC: '+ CAST(@RC AS VARCHAR(10))

/*******************************************************************

Build the source Data

*******************************************************************/

IF @RC = 0
BEGIN
SELECT 'Building Data Source'
EXECUTE @RC = RADB.dbo.CRD_Asthma_Build_DataSource
SELECT 'Complete - Building Data Source ' + CAST(@@ROWCOUNT AS VARCHAR(100)) + ' With RC: '+ CAST(@RC AS VARCHAR(10))

END


/*******************************************************************

Build the Fact Table

*******************************************************************/

IF @RC = 0
BEGIN
SELECT 'Building Encounter Fact'
EXECUTE @RC = RADB.dbo.CRD_Asthma_Build_MetricFact
SELECT 'Complete - Building Encounter Fact ' + CAST(@@ROWCOUNT AS VARCHAR(100)) + ' With RC: '+ CAST(@RC AS VARCHAR(10))

END


SELECT @@ERROR
GO



CREATE PROCEDURE dbo.CRD_ERAS_Create_DateDim
as


/****** Script for SelectTopNRows command from SSMS  ******/

/*I am going to recreate the date table everytime to cover for date dimension changes and metric additions / subtractions*/
IF OBJECT_ID('RADB.dbo.CRD_ERAS_MetDate') IS NOT NULL
/*Then it exists*/
	DROP TABLE RADB.dbo.CRD_ERAS_MetDate 

/*Recreate the table structure*/
		SELECT TOP 0 
		CAST(NULL AS INT) 'MetID'
		, CAST(NULL AS VARCHAR(500)) 'MetName'
		,DD.* 
		INTO RADB.dbo.CRD_ERAS_MetDate   FROM [RADB].[dbo].[Dataview_Dim_Date] dd

/*Run the Cursor through the Metric Dim to give every metric a date in time (>=2012 <=2020).*/

		DECLARE @MetName VARCHAR(75)
		DECLARE @MetId AS int
		DECLARE Met_Cur CURSOR FOR 
		SELECT MetricName,ID FROM [RADB].[dbo].[CRD_ERAS_MetricDim] ORDER BY ID
		OPEN Met_Cur

		FETCH NEXT FROM Met_Cur 
		INTO @MetName, @MetId

		WHILE @@FETCH_STATUS = 0
		BEGIN

		INSERT INTO RADB.dbo.CRD_ERAS_MetDate
		SELECT 
		@MetId
		,@MetName
		,DD.*
		FROM [RADB].[dbo].[Dataview_Dim_Date] dd
		WHERE dd.full_date >= '1/1/2013' AND dd.full_date <= '1/1/2018'


			FETCH NEXT FROM Met_Cur 
			INTO @MetName, @MetId
		END 

		CLOSE Met_Cur;
		DEALLOCATE Met_Cur;



		--SELECT TOP 0 CAST(NULL AS INT) 'MetID', CAST(NULL AS VARCHAR(500)) 'MetName',* INTO RADB.dbo.CRD_Asthma_MetDate FROM [RADB].[dbo].[Dataview_Dim_Date] dd



		--SELECT * FROM RADB.dbo.CRD_Asthma_MetDate 


SELECT * FROM radb.dbo.CRD_ERASOrtho_Cases AS ceoc

--fact view

SELECT * FROM dbo.CRD_ERAS_MetricDim AS cemd
SELECT * FROM dbo.CRD_ERAS_Metricfact AS cemd

IF OBJECT_ID('radb.dbo.CRD_ERAS_Metricfact') IS NOT NULL
	DROP VIEW dbo.CRD_ERAS_Metricfact;
GO

CREATE VIEW dbo.CRD_ERAS_MetricFact
AS

SELECT --median los
		CAST('1' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,Discharge_DateKey AS DateKey
	   ,LOSDays AS Num
	   ,1 'Den'
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim
UNION ALL
SELECT --average los
		CAST('2' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,Discharge_DateKey
	   ,LOSDays AS Num
	   ,1 'Den'
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim

UNION ALL

SELECT --readmission rate
		CAST('3' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,ah.HospitalWide_30DayReadmission_NUM AS Num
	   ,ah.HospitalWide_30DayReadmission_DEN AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim ah


UNION ALL

SELECT --#QVI pneumonia 
		CAST('19' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,ah.qvi_Pneumonia AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim ah

UNION ALL



SELECT --#QVI pneumonia ventilator assoc 
		CAST('48' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,ah.qvi_pnevent AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim ah

UNION ALL


SELECT --#QVI pneumonia aspiration
		CAST('49' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,ah.qvi_pneasp AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim ah

UNION ALL





SELECT --%QVI Any
		CAST('9' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,ah.qvi_Any AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim ah

UNION ALL


SELECT --% ambulate day 0
		CAST('44' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.ambulatepod0,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.ambulatepod0 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah

UNION ALL

SELECT --ambulate pod 1
		CAST('45' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod1 as Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah


UNION ALL

SELECT --ambulate pod 2
		CAST('46' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod2 as Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah


UNION ALL

SELECT --preaadmission counseling
		CAST('41' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.preadm_counseling as Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah

UNION ALL

SELECT --% liquids POD0
		CAST('47' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_pod0 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah




UNION ALL

SELECT --% normal PACU temperature
		CAST('43' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.NormalTempInPacu AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah



UNION ALL

SELECT --% liquids 3 hrs before induction
		CAST('42' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_3ind AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah






UNION ALL
SELECT --# cases
		CAST('37' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID	   
	   ,ah.SurgeryDateKey
	   ,1 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah

UNION ALL

SELECT --# encounters
		CAST('38' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.csn,NULL) 'PAT_ENC_CSN_ID'
	   , NULL AS Log_ID
	   ,ah.Discharge_DateKey
	   ,1 AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_EncDim AS  ah

		sp_who2

		SELECT * FROM radb.dbo.CRD_ERAS_MetricDim AS cemd



		SELECT * FROM INFORMATION_SCHEMA.ROUTINES  WHERE ROUTINE_NAME LIKE '%CRD_ERASOrtho%'
		DROP PROCEDURE CRD_ERASOrtho_DateDim

CREATE PROCEDURE dbo.CRD_ERASOrtho_Create_DateDim
as
/****** Script for SelectTopNRows command from SSMS  ******/




/*I am going to recreate the date table everytime to cover for date dimension changes and metric additions / subtractions*/
IF OBJECT_ID('RADB.dbo.CRD_ERAS_MetDate') IS NOT NULL
/*Then it exists*/
	DROP TABLE RADB.dbo.CRD_ERAS_MetDate 

/*Recreate the table structure*/
		SELECT TOP 0 
		CAST(NULL AS INT) 'MetID'
		, CAST(NULL AS VARCHAR(500)) 'MetName'
		,DD.* 
		INTO RADB.dbo.CRD_ERAS_MetDate   FROM [RADB].[dbo].[Dataview_Dim_Date] dd

/*Run the Cursor through the Metric Dim to give every metric a date in time (>=2012 <=2020).*/

		DECLARE @MetName VARCHAR(75)
		DECLARE @MetId AS int
		DECLARE Met_Cur CURSOR FOR 
		SELECT MetricName,ID FROM [RADB].[dbo].[CRD_ERAS_MetricDim] ORDER BY ID
		OPEN Met_Cur

		FETCH NEXT FROM Met_Cur 
		INTO @MetName, @MetId

		WHILE @@FETCH_STATUS = 0
		BEGIN

		INSERT INTO RADB.dbo.CRD_ERAS_MetDate
		SELECT 
		@MetId
		,@MetName
		,DD.*
		FROM [RADB].[dbo].[Dataview_Dim_Date] dd
		WHERE dd.full_date >= '1/1/2013' AND dd.full_date <= '1/1/2018'


			FETCH NEXT FROM Met_Cur 
			INTO @MetName, @MetId
		END 

		CLOSE Met_Cur;
		DEALLOCATE Met_Cur;

SELECT * FROM radb.dbo.CRD_ERASOrtho_Cases_vw

CREATE VIEW dbo.CRD_ERASOrtho_Cases_vw
AS
SELECT ProcedureType ,
       PAT_NAME ,
       PAT_MRN_ID ,
       pat_id ,
       PAT_ENC_CSN_ID ,
       HSP_ACCOUNT_ID ,
       LOSDays ,
       LOSHours ,
       HOSP_ADMSN_TIME ,
       HOSP_DISCH_TIME ,
       DateKey ,
       ADMISSION_TYPE_C ,
       [Admission Type] ,
       PATIENT_STATUS_C ,
       DISCH_DISP_C ,
       DischargeDisposition ,
       DischargeDisposition2 ,
       Enc_Pat_class_C ,
       Enc_Pat_Class ,
       Surgery_pat_class_c ,
       Surgery_Patient_Class ,
       LOG_ID ,
       STATUS_C ,
       LogStatus ,
       CASE_CLASS_C ,
       [Case Classification] ,
       NUM_OF_PANELS ,
       PROC_DISPLAY_NAME ,
       REAL_CPT_CODE ,
       anescsn ,
       admissioncsn ,
       surgicalcsn ,
       procedurename ,
       Surgery_Room_Name ,
       SurgeonProvid ,
       SurgeonName ,
       ROLE_C ,
       PANEL ,
       ALL_PROCS_PANEL ,
       procline ,
       SurgeryServiceName ,
       SURGERY_DATE AS Surgery_DTTM,
	   CAST(SURGERY_DATE AS DATE) AS SurgeryDate,
       SCHED_START_TIME ,
       SurgeryLocation ,
       setupstart ,
       setupend ,
       inroom ,
       outofroom ,
       cleanupstart ,
       cleanupend ,
       inpacu ,
       outofpacu ,
       inpreprocedure ,
       outofpreprocedure ,
       floorhold ,
       flooroffhold ,
       anesstart ,
       anesfinish ,
       procedurestart ,
       procedurefinish ,
       procedurecarecomplete ,
       postopday1_begin ,
       postopday2_begin ,
       postopday3_begin ,
       postopday4_begin ,
       timeinpacu ,
       pacudelay ,
       HospitalWide_30DayReadmission_NUM ,
       HospitalWide_30DayReadmission_DEN ,
       ambulatepod0 ,
       preopmultimodal ,
       preopmultimodal_nummeds ,
       intraop_spinalanes ,
       intraop_intraartic ,
       intraop_intraartic_nummeds ,
       intraop_departure ,
       postop_painmanage_parent ,
       postop_antiemetics ,
       foleycath 
FROM radb.dbo.CRD_ERASOrtho_Cases

SELECT * FROM INFORMATION_SCHEMA.VIEWS AS v WHERE v.TABLE_NAME LIKE '%ortho%'




SELECT ProcedureType ,
       PAT_NAME ,
       PAT_MRN_ID ,
       pat_id ,
       PAT_ENC_CSN_ID ,
       HSP_ACCOUNT_ID ,
       LOSDays ,
       LOSHours ,
       HOSP_ADMSN_TIME ,
       HOSP_DISCH_TIME ,
       DateKey ,
       ADMISSION_TYPE_C ,
       [Admission Type] ,
       PATIENT_STATUS_C ,
       DISCH_DISP_C ,
       DischargeDisposition ,
       DischargeDisposition2 ,
       Enc_Pat_class_C ,
       Enc_Pat_Class ,
       Surgery_pat_class_c ,
       Surgery_Patient_Class ,
       LOG_ID ,
       STATUS_C ,
       LogStatus ,
       CASE_CLASS_C ,
       [Case Classification] ,
       NUM_OF_PANELS ,
       PROC_DISPLAY_NAME ,
       REAL_CPT_CODE ,
       anescsn ,
       admissioncsn ,
       surgicalcsn ,
       procedurename ,
       Surgery_Room_Name ,
       SurgeonProvid ,
       SurgeonName ,
       ROLE_C ,
       PANEL ,
       ALL_PROCS_PANEL ,
       procline ,
       SurgeryServiceName ,
       Surgery_DTTM ,
       SurgeryDate ,
       SCHED_START_TIME ,
       SurgeryLocation ,
       setupstart ,
       setupend ,
       inroom ,
       outofroom ,
       cleanupstart ,
       cleanupend ,
       inpacu ,
       outofpacu ,
       inpreprocedure ,
       outofpreprocedure ,
       floorhold ,
       flooroffhold ,
       anesstart ,
       anesfinish ,
       procedurestart ,
       procedurefinish ,
       procedurecarecomplete ,
       postopday1_begin ,
       postopday2_begin ,
       postopday3_begin ,
       postopday4_begin ,
       timeinpacu ,
       pacudelay ,
       HospitalWide_30DayReadmission_NUM ,
       HospitalWide_30DayReadmission_DEN ,
       ambulatepod0 ,
       preopmultimodal ,
       preopmultimodal_nummeds ,
       intraop_spinalanes ,
       intraop_intraartic ,
       intraop_intraartic_nummeds ,
       intraop_departure ,
       postop_painmanage_parent ,
       postop_antiemetics ,
       foleycath 
FROM radb.dbo.CRD_ERASOrtho_Cases_vw
