
SELECT * FROM RADB.dbo.GH_eras

SELECT * FROM clarity.dbo.zc_or_status

SELECT orl.STATUS_C,zos.name AS OR_status,COUNT(*)
FROM clarity.dbo.or_log orl
LEFT JOIN clarity.dbo.ZC_OR_STATUS AS zos ON zos.STATUS_C=orl.STATUS_C
GROUP BY orl.STATUS_C,zos.name

----make "universal"

----test patients per Jean
----1.       ZZZCOLON,JUDY MR9007891  surgery date=6-30-15
----2.       ZZZBH,TEST ERAS MR9008532 surgery date=6-25-15
----3.       ZZZCRAIG,ERAS c surgery date=6-29-15
----4.       Zzzeras, Jean c surgery date=6-29-15
----MR9007891
----MR9008532 
----MR9008532 
----MR9008532 

----SELECT CASE_CLASS_C FROM dbo.OR_CASE AS oc
----SELECT * FROM RADB.dbo.GH_eras;


----SELECT * from RADB.dbo.GH_eras
----FROM RADB.dbo.GH_eras
----ORDER BY log_id;

--SELECT * FROM RADB.dbo.GH_eras;

USE Clarity

IF object_id ('RADB.dbo.GH_eras') IS NOT NULL 
DROP TABLE  RADB.dbo.GH_eras;




--purpose:
--1. pull procedures done by CPT code
--2. pull procedure name from panel screen
--3. pull all tracking events (inroom, out of room, )
--4. include the anesthesia csn from F_AN_RECORD_SUMMARY 
--5. primary surgeon only


SELECT
  --rid=ROW_NUMBER() OVER (PARTITION BY or_log.log_id,OR_LOG_ALL_PROC.LINE ORDER BY or_log.log_id,OR_LOG_ALL_PROC.LINE),
  CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS LAP%'  or
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS  LAP%' OR 
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS-LAP%' THEN 'Laparoscopic' 
		    
		    WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS OPEN%' OR
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS  OPEN%' OR 
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS-OPEN%' THEN 'Open' 
			END AS Proctype,
		CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS%'  THEN 'Eras Case'
		ELSE 'Non-ERAS Case' END AS ErasCase,
		CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS%'  THEN 1
		ELSE 0
		END AS ERAS_DEN,
  CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS OPEN%' OR
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS  OPEN%' OR 
				 LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS-OPEN%' 
				 THEN 1 ELSE 0 END AS MET3_DEN		,
  clarity.dbo.PATIENT.PAT_NAME,
  clarity.dbo.patient.PAT_MRN_ID,
  clarity.dbo.patient.pat_id,
    clarity.dbo.PAT_ENC_HSP.PAT_ENC_CSN_ID,
  clarity.dbo.PAT_ENC_HSP.HSP_ACCOUNT_ID,
  clarity.dbo.PAT_ENC_HSP.HOSP_ADMSN_TIME,
  clarity.dbo.PAT_ENC_HSP.HOSP_DISCH_TIME,    
  clarity.dbo.pat_enc_hsp.ADT_PAT_CLASS_C AS Enc_Pat_class_C,
	ZC_PAT_CLASS_Enc.NAME AS Enc_Pat_Class,
	  clarity.dbo.or_log.PAT_TYPE_C AS Surgery_pat_class_c,
  ZC_PAT_CLASS_Surg.NAME AS Surgery_Patient_Class,
  clarity.dbo.OR_LOG.LOG_ID,
  clarity.dbo.or_LOG.STATUS_C,
  zos.NAME AS LogStatus,
    clarity.dbo.or_log.CASE_CLASS_C ,
      zocc.NAME AS CASECLASS_DESCR,   
  clarity.dbo.or_log.NUM_OF_PANELS,
   clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME,   --added
  clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE,
  clarity.dbo.F_AN_RECORD_SUMMARY.AN_52_ENC_CSN_ID AS anescsn,
  clarity.dbo.PAT_OR_ADM_LINK.OR_LINK_CSN AS admissioncsn,
  clarity.dbo.PAT_OR_ADM_LINK.PAT_ENC_CSN_ID AS surgicalcsn,
  clarity.dbo.or_proc.proc_name AS procedurename,
  CLARITY_SER_LOG_ROOM.PROV_NAME AS Surgery_Room_Name,
  CLARITY_SER_Surg.PROV_NAME AS SurgeonName ,
  CLARITY.dbo.OR_LOG_ALL_SURG.ROLE_C  ,
  CLARITY.dbo.OR_LOG_ALL_SURG.PANEL  ,
   CLARITY.dbo.OR_LOG_ALL_PROC.ALL_PROCS_PANEL,
  --CLARITY_SER_Anthesia1.PROV_NAME,
  clarity.dbo.OR_LOG_ALL_PROC.LINE AS procline,
  clarity.dbo.ZC_OR_SERVICE.NAME AS SurgeryServiceName,  
  clarity.dbo.OR_LOG.SURGERY_DATE,
  clarity.dbo.OR_LOG.SCHED_START_TIME,
  clarity.dbo.CLARITY_LOC.LOC_NAME AS SurgeryLocation,
  Setup_Start.TRACKING_TIME_IN AS setupstart,
  Setup_End.TRACKING_TIME_IN AS setupend,
  In_Room.TRACKING_TIME_IN AS inroom,
  Out_of_Room.TRACKING_TIME_IN AS outofroom,
  Cleanup_Start.TRACKING_TIME_IN AS cleanupstart,
  Cleanup_End.TRACKING_TIME_IN AS cleanupend,
  PACU_IN.TRACKING_TIME_IN AS inpacu,
  PACU_OUT.TRACKING_TIME_IN AS outofpacu,
  PRE_PROC_IN.TRACKING_TIME_IN AS inpreprocedure,
  PRE_PROC_OUT.TRACKING_TIME_IN AS outofpreprocedure,
  ANES_START.TRACKING_TIME_IN AS anesstart,
  ANES_FINISH.TRACKING_TIME_IN AS anesfinish,
  PROC_START.TRACKING_TIME_IN AS procedurestart,
  PROC_FINISH.TRACKING_TIME_IN AS procedurefinish
  
INTO RADB.dbo.GH_eras
--INTO ##erasproc

FROM    CLARITY.dbo.OR_LOG 
   left OUTER JOIN CLARITY.dbo.OR_LOG_ALL_PROC ON clarity.dbo.OR_LOG.LOG_ID=clarity.dbo.OR_LOG_ALL_PROC.LOG_ID   
   left OUTER JOIN CLARITY.dbo.OR_PROC ON (CLARITY.dbo.OR_LOG_ALL_PROC.OR_PROC_ID=CLARITY.dbo.OR_PROC.OR_PROC_ID)
   LEFT OUTER JOIN CLARITY.dbo.OR_LOG_ALL_SURG ON (CLARITY.dbo.OR_LOG.LOG_ID=CLARITY.dbo.OR_LOG_ALL_SURG.LOG_ID)
   AND CLARITY.dbo.OR_LOG_ALL_SURG.PANEL=1
   AND CLARITY.dbo.OR_LOG_ALL_SURG.ROLE_C=1
  -- AND CLARITY.dbo.OR_LOG_ALL_SURG.LINE=1
  LEFT OUTER JOIN clarity.dbo.PAT_OR_ADM_LINK ON (CLARITY.dbo.PAT_OR_ADM_LINK.LOG_ID=CLARITY.dbo.OR_LOG.LOG_ID)
   LEFT OUTER JOIN clarity.dbo.PAT_ENC_HSP ON (CLARITY.dbo.PAT_ENC_HSP.PAT_ENC_CSN_ID=CLARITY.dbo.PAT_OR_ADM_LINK.OR_LINK_CSN)
   LEFT OUTER JOIN CLARITY.dbo.PATIENT ON (CLARITY.dbo.PATIENT.PAT_ID=CLARITY.dbo.PAT_ENC_HSP.PAT_ID)
   LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS  ZC_PAT_CLASS_Surg ON (ZC_PAT_CLASS_Surg.ADT_PAT_CLASS_C=CLARITY.dbo.OR_LOG.PAT_TYPE_C)
   LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS  ZC_PAT_CLASS_Enc ON (ZC_PAT_CLASS_Enc.ADT_PAT_CLASS_C=clarity.dbo.pat_enc_hsp.ADT_PAT_CLASS_C )
  LEFT OUTER JOIN clarity.dbo.ZC_OR_CASE_CLASS AS zocc  ON zocc.CASE_CLASS_C=clarity.dbo.or_log.CASE_CLASS_C
  LEFT OUTER JOIN clarity.dbo.ZC_OR_CASE_CLASS AS zoclog  ON zoclog.CASE_CLASS_C=clarity.dbo.or_log.CASE_CLASS_C
  LEFT OUTER JOIN clarity.dbo.OR_PROC_CPT_ID ON clarity.dbo.OR_PROC.OR_PROC_ID=clarity.dbo.OR_PROC_CPT_ID.OR_PROC_ID
  LEFT OUTER JOIN clarity.dbo.CLARITY_SER  CLARITY_SER_Surg ON (clarity.dbo.OR_LOG_ALL_SURG.SURG_ID=CLARITY_SER_Surg.PROV_ID)
   FULL OUTER JOIN clarity.dbo.F_AN_RECORD_SUMMARY ON (clarity.dbo.OR_LOG.LOG_ID=clarity.dbo.F_AN_RECORD_SUMMARY.AN_LOG_ID)
   LEFT OUTER JOIN clarity.dbo.CLARITY_SER  CLARITY_SER_Anthesia1 ON (clarity.dbo.F_AN_RECORD_SUMMARY.AN_RESP_PROV_ID=CLARITY_SER_Anthesia1.PROV_ID)
   LEFT OUTER JOIN clarity.dbo.ZC_OR_SERVICE ON (clarity.dbo.ZC_OR_SERVICE.SERVICE_C=clarity.dbo.OR_LOG.SERVICE_C)
   LEFT OUTER JOIN clarity.dbo.CLARITY_SER  CLARITY_SER_LOG_ROOM ON (CLARITY_SER_LOG_ROOM.PROV_ID=CLARITY.dbo.OR_LOG.ROOM_ID)
   LEFT OUTER JOIN clarity.dbo.CLARITY_LOC ON (clarity.dbo.CLARITY_LOC.LOC_ID=clarity.dbo.OR_LOG.LOC_ID)
   LEFT OUTER JOIN clarity.dbo.ZC_OR_STATUS AS zos   ON zos.STATUS_C=clarity.dbo.OR_LOG.STATUS_C
   LEFT OUTER JOIN clarity.dbo.ZC_CASE_TYPE AS zct ON zct.CASE_TYPE_C=clarity.dbo.or_log.CASE_TYPE_C
   
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME 
  
WHERE
( CASETIME .TRACKING_EVENT_C  = 330  )
  )  Setup_Start ON (Setup_Start.LOG_ID=CLARITY.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 340  )
  )  Setup_End ON (Setup_End.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 60  )
  )  In_Room ON (In_Room.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
  LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 110  )
  )  Out_of_Room ON (Out_of_Room.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 400  )
  )  Cleanup_Start ON (Cleanup_Start.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 410)
  )  Cleanup_End ON (Cleanup_End.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 120)
  )  PACU_IN ON (PACU_IN.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 140)
  )  PACU_OUT ON (PACU_OUT.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 20  )
  )  PRE_PROC_IN ON (PRE_PROC_IN.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 50)
  )  PRE_PROC_OUT ON (PRE_PROC_OUT.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
  --new events
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 70)
  )  ANES_START ON (ANES_START.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 100)
  )  ANES_FINISH ON (ANES_FINISH.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  CLARITY.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 80)
  )  PROC_START ON (PROC_START.LOG_ID=clarity.dbo.OR_LOG.LOG_ID)
  
   LEFT OUTER JOIN ( 
  SELECT CASETIME .LOG_ID,
  CASETIME .TRACKING_TIME_IN
FROM
  clarity.dbo.OR_LOG_CASE_TIMES  CASETIME    
WHERE
( CASETIME .TRACKING_EVENT_C  = 390)
  )  PROC_FINISH ON (PROC_FINISH.LOG_ID=CLARITY.dbo.OR_LOG.LOG_ID)

  
WHERE --clarity.dbo.or_log.LOG_ID=436370
   --dbo.OR_LOG.SURGERY_DATE >='6/23/2015' -- for testing jean's patients
	--AND dbo.patient.PAT_MRN_ID IN ('MR9000797','MR9004344','MR9007891','MR9008532','MR9004514',
	--'MR9005155','MR9002249') 
 CLARITY.dbo.pat_enc_hsp.HOSP_DISCH_TIME>'7/1/2015' --for PROD   
--   AND     clarity.dbo.CLARITY_LOC.LOC_NAME  IN  ( 'BH MAIN OR'  )
   AND clarity.dbo.Clarity_LOC.Loc_NAME ='GH MAIN OR'
--   or_log.LOG_ID =446354 IN (SELECT  an_log_id FROM dbo.F_AN_RECORD_SUMMARY AS an WHERE an.AN_52_ENC_CSN_ID=117763167)
   AND clarity.dbo.or_log.STATUS_C IN (2,3,5)
   --AND   dbo.OR_LOG_ALL_SURG.ROLE_C  =  1
   --AND   dbo.OR_LOG_ALL_SURG.PANEL  IN  ( 1  )
AND clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE  IN  
      ('44144','44145','44146','44147','44150','44156','44160','44204',
'44205','44206','44207','44208','44210','44211','44212','44620','44625','44626',
'45110','45111','45112','45113','45114','45116','45119','45120','45121','45123','45126','45130','45160','45550','44157','44158')


SELECT * FROM radb.dbo.CRD_ERAS_CPT_Dim AS cecd

ORDER BY clarity.dbo.or_log.LOG_ID,clarity.dbo.OR_LOG_ALL_PROC.line

--SELECT proc_code AS 'CPT Code' ,PROC_NAME AS CPT_Description
--FROM dbo.CLARITY_EAP AS ce
--WHERE proc_code IN 

--      ('44140','44141','44143','44144','44145','44146','44160','44204','44205','44206','44207','44208','44210',
--'44156','44211','44212','45110','45112','45113','45114','45116','45119','45120','45121','45123','45126','45130',
--'45160','45395','45397','45402','44620','44625','44626','44150','45550','45111','44147','44121','44157','44158')  


--SELECT * 
--FROM dbo.CLARITY_LOC AS cl
--WHERE loc_name LIKE '%GH %'
--ORDER BY loc_name

--SELECT TOP 1000 * 
--FROM dbo.OR_PROC_CPT_ID AS opci



IF object_id('tempdb..##procedurefactstage') IS NOT NULL
	DROP TABLE ##procedurefactstage;
	
	
IF object_id('radb.dbo.ERAS_ProcedureFact') IS NOT NULL
	DROP TABLE radb.dbo.GH_ERAS_ProcedureFact;
	

SELECT rid=ROW_NUMBER() OVER(PARTITION BY log_id,procline ORDER BY log_id)
,* 
INTO ##procedurefactstage
FROM RADB.dbo.GH_eras f
;


--delete duplicate log/line id combinations
DELETE ##procedurefactstage
WHERE rid>1;

SELECT *
INTO radb.dbo.GH_ERAS_ProcedureFact
FROM ##procedurefactstage ;

ALTER TABLE radb.dbo.GH_ERAS_ProcedureFact
DROP COLUMN rid;

--build casefact
IF object_id('radb.dbo.ERAS_CaseFact') IS NOT NULL
	DROP TABLE radb.dbo.GH_ERAS_CaseFact;

SELECT  
       PAT_NAME
,       PAT_MRN_ID
,       pat_id
,       LOG_ID
,       STATUS_C
,       CASE_CLASS_C
,       CASECLASS_DESCR
,       anescsn
,       admissioncsn
,       surgicalcsn
,		proctype
,		erascase
,		eras_den
,		met3_den
,       HSP_ACCOUNT_ID
,       HOSP_ADMSN_TIME
,       HOSP_DISCH_TIME
,       Surgery_Patient_Class
,       Surgery_Room_Name
,       SurgeonName
,       SurgeryServiceName
,		PROC_DISPLAY_NAME
,       SURGERY_DATE
,       SCHED_START_TIME
,       SurgeryLocation
,       setupstart
,       setupend
,       inroom
,       outofroom
,       cleanupstart
,       cleanupend
,       inpacu
,       outofpacu
,       inpreprocedure
,       outofpreprocedure
,       anesstart
,       anesfinish
,       procedurestart
,       procedurefinish
,		case_length_mi=DATEDIFF(mi,inpreprocedure,outofroom)
,		case_length_hrs=DATEDIFF(hh,inpreprocedure,outofroom)
,		pod0_start=SURGERY_DATE
,		pod1_start = DATEADD(dd,1,SURGERY_DATE)
,		pod1_2pm=DATEADD(hh,14,DATEADD(dd,1,SURGERY_DATE))
,		pod2_start= DATEADD(dd,2,SURGERY_DATE)
,		pod3_start=DATEADD(dd,3,SURGERY_DATE)

INTO radb.dbo.GH_ERAS_CaseFact
FROM	(select *,logline=ROW_NUMBER() OVER(PARTITION BY log_id ORDER BY log_id,procline)
		FROM radb.dbo.GH_ERAS_ProcedureFact
		) procfact
WHERE logline=1	
	;



ALTER TABLE radb.dbo.GH_ERAS_CaseFact
ADD
 met1 TINYINT,
met2 TINYINT,
met3 TINYINT,
met4 TINYINT,
met5 TINYINT,
met6 TINYINT,
met7 TINYINT,
met8 TINYINT,
met9 TINYINT,
met10 TINYINT,
met11 TINYINT,
met12 TINYINT,
met12orders TINYINT,
met12flow TINYINT,
met13 TINYINT,
met14 TINYINT,
met15 TINYINT,
met15date DATETIME,
met16 TINYINT,
met16date DATETIME,
met17 TINYINT,
met17date DATETIME,
weight_oz DECIMAL(13,4),
weight_kg DECIMAL(13,4),
iv_totalvolume INT,
iv_totalvolume_intraop INT,
IV_intraop_threshold DECIMAL(13,4),
intraopflag TINYINT,
blocktype VARCHAR(50),
spinelevel VARCHAR(50),
blocktime DATETIME
