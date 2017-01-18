--********************* begin population pull **********************************************************


IF object_id('tempdb.dbo.#GHGIlog') IS NOT NULL
	DROP TABLE #GHGIlog;


SELECT DISTINCT clarity.dbo.OR_LOG.LOG_ID
INTO #GHGIlog
FROM 
 clarity.dbo.OR_LOG 
   left OUTER JOIN CLARITY.dbo.OR_LOG_ALL_PROC  ON clarity.dbo.OR_LOG.LOG_ID=clarity.dbo.OR_LOG_ALL_PROC.LOG_ID   
   left OUTER JOIN CLARITY.dbo.OR_PROC  ON (CLARITY.dbo.OR_LOG_ALL_PROC.OR_PROC_ID=CLARITY.dbo.OR_PROC.OR_PROC_ID)   
  LEFT OUTER JOIN clarity.dbo.OR_PROC_CPT_ID ON clarity.dbo.OR_PROC.OR_PROC_ID=clarity.dbo.OR_PROC_CPT_ID.OR_PROC_ID
  JOIN RADB.dbo.CRD_ERAS_GHGI_CptList AS cptdim ON cptdim.CPTCode=clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE;



IF object_id('tempdb..#GHGIallprocs') is not null
	drop table #GHGIallprocs; 


SELECT   
  clarity.dbo.OR_LOG.LOG_ID,  
  clarity.dbo.PATIENT.PAT_NAME,
  clarity.dbo.patient.PAT_MRN_ID,
  clarity.dbo.patient.pat_id,  
  clarity.dbo.PAT_ENC_HSP.HOSP_DISCH_TIME,
  clarity.dbo.PAT_ENC_HSP.HOSP_ADMSN_TIME,
  at.name AS AdmitType,
  clarity.dbo.or_log.PAT_TYPE_C AS Surgery_pat_class_c,
  ZC_PAT_CLASS_Surg.NAME AS Surgery_Patient_Class,  
  clarity.dbo.or_LOG.STATUS_C,
  zos.NAME AS LogStatus,
  clarity.dbo.or_log.CASE_CLASS_C ,
  zocc.NAME AS CASECLASS_DESCR,   
  clarity.dbo.or_log.NUM_OF_PANELS,
  clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME,   --added
  ErasProcedure=CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS%'  THEN 'Eras Procedure'
		ELSE 'Non-ERAS Procedure' END ,
  ErasCase=CASE WHEN LTRIM(clarity.dbo.OR_LOG_ALL_PROC.PROC_DISPLAY_NAME) LIKE 'ERAS%'  THEN 'Eras Case'
		ELSE 'Non-ERAS Case' END ,
  clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE,
  CASE WHEN cptdim.CPTCode IS NOT NULL THEN 'Yes' ELSE 'No' END AS InCPTList,
  cptdim.CPTCode,
  cptdim.CPT_Description,
  cptdim.OpenVsLaparoscopic,  
  clarity.dbo.F_AN_RECORD_SUMMARY.AN_52_ENC_CSN_ID AS anescsn,
  clarity.dbo.PAT_OR_ADM_LINK.OR_LINK_CSN AS admissioncsn,
  clarity.dbo.PAT_OR_ADM_LINK.PAT_ENC_CSN_ID AS surgicalcsn,
  clarity.dbo.or_proc.proc_name AS procedurename,
  CLARITY_SER_LOG_ROOM.PROV_NAME AS Surgery_Room_Name,
  CLARITY_SER_Surg.PROV_NAME AS SurgeonName ,
  allsurg.ROLE_C  ,
  allsurg.PANEL  ,
   CLARITY.dbo.OR_LOG_ALL_PROC.ALL_PROCS_PANEL,
  --CLARITY_SER_Anthesia1.PROV_NAME,
  clarity.dbo.OR_LOG_ALL_PROC.LINE AS ProcLineNumber,
  clarity.dbo.ZC_OR_SERVICE.NAME AS SurgeryServiceName,  
  clarity.dbo.OR_LOG.SURGERY_DATE,
  DateKey=RADB.dbo.fn_Generate_DateKey(clarity.dbo.OR_LOG.SURGERY_DATE) ,
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
  PROC_FINISH.TRACKING_TIME_IN AS procedurefinish,
  postopday1_begin=CONVERT(DATETIME,CONVERT(DATE,DATEADD(dd,1,PROC_START.TRACKING_TIME_IN))),
  postopday2_begin=CONVERT(DATETIME,CONVERT(DATE,DATEADD(dd,2,PROC_START.TRACKING_TIME_IN))),
  postopday3_begin=CONVERT(DATETIME,CONVERT(DATE,DATEADD(dd,3,PROC_START.TRACKING_TIME_IN))),
  postopday4_begin=CONVERT(DATETIME,CONVERT(DATE,DATEADD(dd,3,PROC_START.TRACKING_TIME_IN))),
  CaseLength_min=CAST(NULL AS int),
  CaseLength_hrs=CAST(NULL AS NUMERIC(13,4)),
  timeinpacu_min=NULL,
  pacudelay=NULL
  
INTO #GHGIallprocs

FROM    clarity.dbo.OR_LOG 
  INNER JOIN #GHGIlog AS ghlog ON ghlog.LOG_ID = OR_LOG.LOG_ID
   left OUTER JOIN CLARITY.dbo.OR_LOG_ALL_PROC  ON clarity.dbo.OR_LOG.LOG_ID=clarity.dbo.OR_LOG_ALL_PROC.LOG_ID   
   left OUTER JOIN CLARITY.dbo.OR_PROC  ON (CLARITY.dbo.OR_LOG_ALL_PROC.OR_PROC_ID=CLARITY.dbo.OR_PROC.OR_PROC_ID)
   LEFT OUTER JOIN 
	(SELECT allsurg.*
	FROM CLARITY.dbo.OR_LOG_ALL_SURG AS allsurg
	JOIN (SELECT log_Id,MAX(line) AS maxline
		  FROM CLARITY.dbo.OR_LOG_ALL_SURG AS allsurg
		  WHERE PANEL=1
		  AND  ROLE_C=1 
		  GROUP BY log_id   ) AS maxsurg ON maxsurg.LOG_ID=allsurg.LOG_ID
											AND maxsurg.maxline=allsurg.line
	) AS allsurg ON allsurg.LOG_ID=clarity.dbo.or_log.LOG_ID 
	
  LEFT OUTER JOIN clarity.dbo.PAT_OR_ADM_LINK ON (CLARITY.dbo.PAT_OR_ADM_LINK.LOG_ID=CLARITY.dbo.OR_LOG.LOG_ID)
  LEFT OUTER JOIN clarity.dbo.PAT_ENC_HSP ON (CLARITY.dbo.PAT_ENC_HSP.PAT_ENC_CSN_ID=CLARITY.dbo.PAT_OR_ADM_LINK.OR_LINK_CSN) 
  LEFT OUTER JOIN clarity.dbo.HSP_ACCOUNT AS hsp ON hsp.HSP_ACCOUNT_ID=clarity.dbo.PAT_ENC_HSP.HSP_ACCOUNT_ID
  LEFT OUTER JOIN clarity.dbo.HSP_ACCOUNT_3 AS hsp3 ON hsp3.HSP_ACCOUNT_ID=clarity.dbo.PAT_ENC_HSP.HSP_ACCOUNT_ID
  LEFT JOIN Clarity.dbo.ZC_HOSP_ADMSN_TYPE AT ON HSP3.ADMIT_TYPE_EPT_C = at.HOSP_ADMSN_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS  ZC_PAT_CLASS_Surg ON (ZC_PAT_CLASS_Surg.ADT_PAT_CLASS_C=CLARITY.dbo.OR_LOG.PAT_TYPE_C)   
  LEFT OUTER JOIN clarity.dbo.ZC_OR_CASE_CLASS AS zocc  ON zocc.CASE_CLASS_C=clarity.dbo.or_log.CASE_CLASS_C
  LEFT OUTER JOIN clarity.dbo.ZC_OR_CASE_CLASS AS zoclog  ON zoclog.CASE_CLASS_C=clarity.dbo.or_log.CASE_CLASS_C
  LEFT OUTER JOIN clarity.dbo.OR_PROC_CPT_ID ON clarity.dbo.OR_PROC.OR_PROC_ID=clarity.dbo.OR_PROC_CPT_ID.OR_PROC_ID
  JOIN radb.dbo.CRD_ERAS_GHGI_CptList AS cptdim ON cptdim.CPTCode=clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE
  							
  LEFT OUTER JOIN CLARITY.dbo.PATIENT ON (CLARITY.dbo.PATIENT.PAT_ID=CLARITY.dbo.PAT_ENC_HSP.PAT_ID)
  LEFT OUTER JOIN clarity.dbo.CLARITY_SER  CLARITY_SER_Surg ON (allsurg.SURG_ID=CLARITY_SER_Surg.PROV_ID)
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

  
WHERE  (clarity.dbo.OR_LOG.SURGERY_DATE>='1/1/2015' )
 AND     clarity.dbo.CLARITY_LOC.LOC_NAME  IN  ( 'GH MAIN OR')
  
   AND clarity.dbo.or_log.STATUS_C IN (2,5)
   AND PAT_ENC_HSP.HOSP_DISCH_TIME IS NOT NULL
   AND hsp.ACCT_BASECLS_HA_C=1 --inpatient
   AND clarity.dbo.or_log.PAT_TYPE_C IN ('108','101')
   

ORDER BY clarity.dbo.or_log.LOG_ID,clarity.dbo.OR_LOG_ALL_PROC.line;


TRUNCATE TABLE RADB.dbo.CRD_ERAS_GHGI_AllProc;



--create all proc table 
INSERT	RADB.dbo.CRD_ERAS_GHGI_AllProc
        ( LOG_ID
        ,ProcLineNumber
        ,PROC_DISPLAY_NAME
        ,ErasProcedure        
        ,CPTCode
        ,OpenVsLaparoscopic
        ,InCPTList
        )
SELECT LOG_ID
        ,ProcLineNumber
        ,PROC_DISPLAY_NAME
        ,ErasProcedure
        ,CPTCode
        ,OpenVsLaparoscopic
        ,InCPTList
FROM    #GHGIallprocs AS gi;


IF object_id('RADB.dbo.CRD_ERAS_GHGI_Case') IS NOT NULL
	DROP TABLE RADB.dbo.CRD_ERAS_GHGI_Case;


WITH basecase AS (
SELECT   
 logseq=ROW_NUMBER() OVER (PARTITION BY LOG_ID ORDER BY LOG_ID)
,LOG_ID
,      PAT_NAME
,      PAT_MRN_ID
,      PAT_ID
,      HOSP_DISCH_TIME
,      HOSP_ADMSN_TIME
,      AdmitType
,      Surgery_pat_class_c
,      Surgery_Patient_Class
,      STATUS_C
,      LogStatus
,      CASE_CLASS_C
,      CASECLASS_DESCR
,      NUM_OF_PANELS
,      PROC_DISPLAY_NAME
,      ErasProcedure
,      ErasCase
,      REAL_CPT_CODE
,      InCPTList
,      CPTCode
,      CPT_Description
,      OpenVsLaparoscopic
,      anescsn
,      admissioncsn
,      surgicalcsn
,      procedurename
,      Surgery_Room_Name
,      SurgeonName
,      ROLE_C
,      PANEL
,      ALL_PROCS_PANEL
,      ProcLineNumber
,      SurgeryServiceName
,      SURGERY_DATE
,      DateKey
,      SCHED_START_TIME
,      SurgeryLocation
,      setupstart
,      setupend
,      inroom
,      outofroom
,      cleanupstart
,      cleanupend
,      inpacu
,      outofpacu
,      inpreprocedure
,      outofpreprocedure
,      anesstart
,      anesfinish
,      procedurestart
,      procedurefinish
,      postopday1_begin
,      postopday2_begin
,      postopday3_begin
,      postopday4_begin
,      CaseLength_min
,      CaseLength_hrs
,      timeinpacu_min
,      pacudelay
  --begin process metrics
  ,preadm_counseling = 0
  ,         pacutemp = CAST(NULL AS NUMERIC(13, 2))
  ,         NormalTempInPacu = CAST(NULL AS TINYINT)
  ,         ambulatepod0 = CAST(0 AS TINYINT)
  ,         lumbar_epi = CAST(0 AS TINYINT)
  ,         clearliquids_3ind = CAST(0 AS TINYINT)
  ,         clearliquids_pod0 = CAST(0 AS TINYINT)
  ,         clearliquids_pod1 = CAST(0 AS TINYINT)
  ,         ambulate_pod1 = CAST(0 AS TINYINT)
  ,         solidfood_pod1 = CAST(0 AS TINYINT)
  ,         solidfood_pod2 = CAST(0 AS TINYINT)
  ,         ambulate_pod2 = CAST(0 AS TINYINT)
  ,         date_toleratediet = CAST(NULL AS DATETIME)
  ,         hrs_toleratediet = CAST(NULL AS INT)
  ,         date_bowelfunction = CAST(NULL AS DATETIME)
  ,         hrs_tobowelfunction = CAST(NULL AS INT)
  ,         postop_painiv_count = CAST(NULL AS INT)
  ,         postop_paintotal_count = CAST(NULL AS INT)
  ,         iv_totalvolume_intraop = CAST(NULL AS INT)
  ,         iv_intraop_threshold = CAST(NULL AS DECIMAL(13, 4))
  ,         goal_guidelines = CAST (NULL AS TINYINT)
  ,         mm_antiemetic_intraop = CAST(0 AS TINYINT)
  ,         date_last_IVpainmed = CAST(NULL AS DATETIME)
  ,         hrs_last_IVpainmed = CAST(NULL AS INT)
  ,         nameof_last_IVpainmed = CAST(NULL AS VARCHAR(100))
  ,         last_IVpainmed_adminby = CAST(NULL AS VARCHAR(100))
  ,         tapblock_placed_flag = CAST(NULL AS TINYINT)
  ,         taps_timeplaced = CAST(NULL AS DATETIME)
  ,         taps_provider = CAST(NULL AS VARCHAR(100))
  ,         taps_orderid = CAST(NULL AS NUMERIC(18, 0))
  ,         log_orderset_flag = CAST(NULL AS TINYINT)
  ,         foleypod1_Num = CAST(0 AS INT)
,			foleypod_Den = cast (0 AS INT)
,			foleysuprapubic_flag =CAST(0 AS int)
FROM #GHGIallprocs AS gi	
)SELECT * 
INTO RADB.dbo.CRD_ERAS_GHGI_Case
FROM basecase
WHERE logseq=1;



UPDATE RADB.dbo.CRD_ERAS_GHGI_Case
SET CaseLength_hrs=DATEDIFF(HOUR,inroom,outofroom);

UPDATE RADB.dbo.CRD_ERAS_GHGI_Case
SET CaseLength_min=DATEDIFF(MINUTE,inroom,outofroom);


IF object_id('RADB.dbo.CRD_ERAS_GHGI_EncDim') IS NOT NULL
	DROP TABLE RADB.dbo.CRD_ERAS_GHGI_EncDim;


SELECT  
  peh.PAT_ENC_CSN_ID,  
  peh.HSP_ACCOUNT_ID,
  p.PAT_NAME,
  p.PAT_MRN_ID,
  LOSDays=DATEDIFF(dd,peh.HOSP_ADMSN_TIME,peh.HOSP_DISCH_TIME)    ,
  LOSHours=DATEDIFF(hh,peh.HOSP_ADMSN_TIME,peh.HOSP_DISCH_TIME)    ,
  peh.HOSP_ADMSN_TIME,
  peh.HOSP_DISCH_TIME,    
  Discharge_DateKey=RADB.dbo.fn_Generate_DateKey(peh.HOSP_DISCH_TIME) ,
  peh.DISCH_DISP_C,
  zdd.name AS Enc_DischargeDisposition,
  hsp.PATIENT_STATUS_C,
  zcpat.NAME AS PatientStatus,  
  peh.ADT_PAT_CLASS_C AS Enc_Pat_class_C,
  hsp.ACCT_BASECLS_HA_C,
  basecl.name AS BaseClass,
	ZC_PAT_CLASS_Enc.NAME AS Enc_Pat_Class,
	hsp.ADMISSION_TYPE_C,
  zadm.name AS [Admission Type],
  ra.HospitalWide_30DayReadmission_NUM,
  ra.HospitalWide_30DayReadmission_DEN,
  NumberofProcs=CAST(NULL AS INT),
  rdc.TotalDirectCost,
  CAST(0 AS INT) AS ED_revisit,
  CAST(0 AS INT) AS OrdersetFlag,
  FirstOS_ordernumber=CAST(NULL AS NUMERIC(18,0)),
  FirstOS_ordername = CAST(NULL AS VARCHAR(510)),
  FirstOS_orderdate= CAST(NULL AS DATETIME),
   patient_weight_oz=CAST(NULL AS DECIMAL(13,4)),
  patient_weight_kg=CAST(NULL AS DECIMAL(13,4)),
  --new groupers
  ec.SurgeonName,
  ec.ErasCase,
  ec.OpenVsLaparoscopic

 INTO RADB.dbo.CRD_ERAS_GHGI_EncDim
FROM clarity.dbo.PAT_ENC_HSP AS peh 
   JOIN  (SELECT rid=ROW_NUMBER() OVER (PARTITION BY admissioncsn ORDER BY admissioncsn)
		 ,*
		FROM RADB.dbo.CRD_ERAS_GHGI_Case ) AS ec ON ec.admissioncsn=peh.PAT_ENC_CSN_ID   
												AND rid=1
   LEFT JOIN clarity.dbo.HSP_ACCOUNT  AS hsp ON peh.HSP_ACCOUNT_ID=hsp.HSP_ACCOUNT_ID
   LEFT JOIN clarity.dbo.ZC_ACCT_BASECLS_HA AS basecl ON basecl.ACCT_BASECLS_HA_C=hsp.ACCT_BASECLS_HA_C
   LEFT JOIN clarity.dbo.ZC_MC_PAT_STATUS AS zcpat ON zcpat.PAT_STATUS_C=hsp.PATIENT_STATUS_C
   LEFT JOIN clarity.dbo.ZC_DISCH_DISP AS zdd ON zdd.DISCH_DISP_C=peh.DISCH_DISP_C
   LEFT JOIN radb.dbo.ReDiscovery_Costs AS rdc ON rdc.HSP_ACCOUNT_ID=peh.HSP_ACCOUNT_ID   
   LEFT OUTER JOIN CLARITY.dbo.PATIENT AS p ON (p.PAT_ID=peh.PAT_ID)
   LEFT JOIN radb.dbo.ReadmissionFact ra    
	ON CONVERT (varchar(30),peh.HSP_ACCOUNT_ID)=ra.IDX_VisitNum  
  LEFT OUTER JOIN clarity.dbo.ZC_MC_ADM_TYPE AS zadm ON zadm.ADMISSION_TYPE_C=hsp.ADMISSION_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS  ZC_PAT_CLASS_Enc ON (ZC_PAT_CLASS_Enc.ADT_PAT_CLASS_C=peh.ADT_PAT_CLASS_C );

  UPDATE RADB.dbo.CRD_ERAS_GHGI_EncDim
  SET NumberofProcs=procct.ct
  FROM RADB.dbo.CRD_ERAS_GHGI_EncDim AS e
  LEFT JOIN 
  ( SELECT admissioncsn,COUNT(*) AS ct
    FROM RADB.dbo.CRD_ERAS_GHGI_Case 
	GROUP BY admissioncsn
	) AS procct ON procct.admissioncsn=e.PAT_ENC_CSN_ID;



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
			 ,s.admissioncsn'Idx_CSN'
			 ,ed.ED_ARRIVAL_TIME
			 ,s.SURGERY_DATE
			 ,DATEDIFF(DAY,s.SURGERY_DATE,ed.ED_ARRIVAL_TIME) 'EDvisit'
			 ,ED.ARRIVAL_LOCATION
			 ,ED.ED_DISPOSITION
		
		

			 From
			 [RADB].[dbo].[vw_YNHHS_ED_DATA] ED
			 JOIN (SELECT admissioncsn, PAT_ID, SURGERY_DATE FROM radb.dbo.CRD_ERAS_GHGI_Case  GROUP BY admissioncsn, PAT_ID, SURGERY_DATE) S ON ED.PAT_ID = s.PAT_ID AND ed.ED_ARRIVAL_TIME > s.SURGERY_DATE
			 ) x
		JOIN radb.dbo.CRD_ERAS_GHGI_Case s2 ON  s2.admissioncsn= x.Idx_CSN 
						--AND x.Idx_CSN = x.PAT_ENC_CSN_ID 
						--AND s2.AdmitType <> 'Emergency'
		WHERE EDvisit <= 30
				and	0 = CASE WHEN (x.PAT_ENC_CSN_ID = x.Idx_CSN AND s2.AdmitType = 'Emergency') THEN 1 ELSE 0 END 
				
	 
  UPDATE RADB.dbo.CRD_ERAS_GHGI_EncDim
  SET ED_revisit=CASE WHEN ed.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END 
  FROM RADB.dbo.CRD_ERAS_GHGI_EncDim AS e
  LEFT JOIN 
  ( SELECT PAT_ENC_CSN_ID
    FROM #EDRevisit 
	GROUP BY PAT_ENC_CSN_ID
	) AS ed ON ed.PAT_ENC_CSN_ID=e.PAT_ENC_CSN_ID;

	
--orderset flag
WITH orderset AS (

SELECT 
rid=ROW_NUMBER() OVER (PARTITION BY om.PAT_ENC_CSN_ID ORDER BY order_dttm)
,ec.PAT_ENC_CSN_ID
,om.ORDER_DTTM
,om.ORDER_ID
,om.DISPLAY_NAME AS 'Order_Display_Name'
,om.ORDER_TYPE_C

FROM   RADB.dbo.CRD_ERAS_GHGI_EncDim AS ec
JOIN  clarity.dbo.ORDER_METRICS AS om ON om.PAT_ENC_CSN_ID=ec.PAT_ENC_CSN_ID
WHERE om.PRL_ORDERSET_ID=3040002508
) UPDATE RADB.dbo.CRD_ERAS_GHGI_EncDim
SET FirstOS_ordernumber=o.ORDER_ID
,FirstOS_ordername=o.Order_Display_Name
,FirstOS_orderdate=o.ORDER_DTTM
,OrdersetFlag=1
FROM RADB.dbo.CRD_ERAS_GHGI_EncDim AS enc
JOIN orderset AS o ON o.PAT_ENC_CSN_ID=enc.PAT_ENC_CSN_ID
WHERE o.rid=1;
--update log orderset flag
UPDATE radb.dbo.CRD_ERAS_GHGI_Case
SET log_orderset_flag=1
FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
JOIN radb.dbo.CRD_ERAS_GHGI_EncDim AS e ON c.admissioncsn=e.PAT_ENC_CSN_ID
WHERE e.OrdersetFlag=1;


