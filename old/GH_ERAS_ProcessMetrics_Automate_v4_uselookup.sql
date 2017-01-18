
---********************************* start main code ***************


IF object_id('tempdb..##erasfact') IS NOT NULL
	DROP TABLE ##erasfact;
	
SELECT admissioncsn AS pat_enc_csn_id ,* 
INTO ##erasfact
FROM radb.dbo.GH_ERAS_CaseFact 
WHERE admissioncsn IS NOT null
UNION 
SELECT surgicalcsn ,* 
FROM radb.dbo.GH_ERAS_CaseFact 
WHERE surgicalcsn IS NOT null
UNION 
SELECT anescsn ,* 
FROM radb.dbo.GH_ERAS_CaseFact 
WHERE anescsn IS NOT NULL;



UPDATE ##erasfact
SET met1=0,met2=0,met3=0,met4=0,met5=0,met6=0,
met7=0,met8=0,met9=0,met10=0,met11=0,met12=0,met12orders=0,met12flow=0,met13=0,
met14=0,met15=0,met16=0,met17=0,met15date=NULL,met16date=NULL,met17date=null;



USE CLARITY;
--metric 1 pre-admission counseling
WITH baseq AS (
SELECT  b.PAT_NAME
,       b.PAT_MRN_ID
,       b.HOSP_ADMSN_TIME
,       b.HOSP_DISCH_TIME
,       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.admissioncsn AS pat_enc_csn_id
,       ifm.FSD_ID
,       ifm.RECORDED_TIME
,       ifm.MEAS_VALUE
,       ifm.MEAS_COMMENT
--,		peh.INPATIENT_DATA_ID
,		b.SURGERY_DATE
,		b.anesstart
,		DATEADD(hh,-3,anesstart) AS anesstart_minus3h
,		CASE WHEN 
		ifm.RECORDED_TIME<b.HOSP_ADMSN_TIME THEN 1 ELSE 0 
		END AS mettime
,		rid=ROW_NUMBER () OVER (PARTITION BY b.admissioncsn ORDER BY b.admissioncsn)
FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact b
        ON ids.EPT_CSN=b.pat_enc_csn_id
        LEFT JOIN clarity.dbo.PATIENT AS p ON b.PAT_ID = p.PAT_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C

WHERE   ifm.FLO_MEAS_ID IN ( '10713938' )
        AND (ifm.RECORDED_TIME>=DATEADD(dd,-7,b.HOSP_ADMSN_TIME) AND ifm.RECORDED_TIME<b.HOSP_ADMSN_TIME)
),csnlist AS (
SELECT pat_enc_csn_id
from baseq
WHERE meas_value='Yes'
GROUP BY pat_enc_Csn_id
)--SELECT * FROM csnList
UPDATE ##erasfact
SET met1=1
FROM ##erasfact AS f
JOIN csnlist csn
ON f.pat_enc_csn_id=csn.pat_enc_csn_id;

--metric 2 - clear liquids 3 hours before induction
WITH baseq AS (
SELECT  p.PAT_NAME
,       p.PAT_MRN_ID
,       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,       ifm.FSD_ID
,       ifm.RECORDED_TIME
,		ifm.flo_meas_id
,       ifm.MEAS_VALUE
,       ifm.MEAS_COMMENT
,		datelastliquid=CASE WHEN ifm.flo_meas_id=1020100004 THEN   CONVERT(DATEtime,DATEADD(dd,CONVERT(int,ifm.MEAS_VALUE),'12/31/1840')) END
,		b.SURGERY_DATE
,		b.anesstart
,		DATEADD(hh,-3,anesstart) AS anesstart_minus3h
,		CASE WHEN 	ifm.RECORDED_TIME>=b.SURGERY_DATE AND ifm.RECORDED_TIME<=DATEADD(hh,-3,anesstart) THEN 1 ELSE 0 
		END AS mettime
,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY b.pat_enc_csn_id)
FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact AS b
        ON ids.EPT_CSN=b.pat_enc_csn_id
        LEFT JOIN clarity.dbo.PATIENT AS p ON b.pat_id = p.PAT_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
WHERE   --peh.PAT_ENC_CSN_ID IN (SELECT pat_enc_csn_id FROM  RADB.dbo.BH_eras)
        ifm.FLO_MEAS_ID IN ( '1020100004', '1217' ) --1020100004 -date 1217 time
--        AND (ifm.RECORDED_TIME>=b.SURGERY_DATE AND ifm.RECORDED_TIME<=DATEADD(hh,-3,anesstart))
) 
, alldocs AS 
 (SELECT b.pat_enc_csn_id
FROM baseq b
JOIN (SELECT pat_enc_csn_id ,MAX(rid) AS maxrid
	  FROM baseq
	  WHERE rid>1
	  GROUP BY pat_enc_csn_id ) x ON b.pat_enc_csn_id=x.pat_enc_csn_id
	  AND b.rid=x.maxrid
), dt as
(SELECT pAT_enc_csn_id,datelastliquid 
 FROM 
 baseq
 WHERE flo_meas_id='1020100004'
), tm   as
(SELECT pAT_enc_csn_id,CONVERT(int,meas_value) AS sec
 FROM 
 baseq
 WHERE flo_meas_id='1217'
) ,dttm AS (
SELECT d.*,t.sec,dttm_lastliquid=DATEADD(ss,t.sec,datelastliquid)
from dt d
JOIN tm t
ON d.pat_enc_csn_id=t.pat_enc_csn_id
)
----SELECT b.surgery_date,a.datelastliquid,a.dttm_lastliquid,b.pat_name,b.pat_mrn_id,b.anesstart,b.anesstart_minus3h 
----FROM dttm a
----JOIN ##erasfact AS f ON a.pat_enc_csn_id=f.pat_enc_csn_id
----JOIN baseq AS b ON b.pat_enc_csn_id=f.pat_enc_csn_id
----ORDER BY a.pat_enc_csn_id
--SELECT * FROM baseq ORDER BY pat_enc_csn_id,recorded_time
UPDATE ##erasfact
SET met2=1
--SELECT f.*,d.dttm_lastliquid
FROM ##erasfact AS f
JOIN dttm AS d
ON f.pat_enc_csn_id=d.pat_enc_csn_id
WHERE d.dttm_lastliquid>=f.SURGERY_DATE AND d.dttm_lastliquid<=DATEADD(hh,-3,f.anesstart);

--metric 3 placeholder - thoracic epidural


IF object_id('tempdb..##epi') IS NOT NULL
	DROP TABLE ##epi;


WITH basesmart AS (	
SELECT  ev.HLV_ID
,		op.PAT_ENC_CSN_ID
,		cc.ABBREVIATION AS ElementName
,		sed.ELEMENT_ID
,		ev.SMRTDTA_ELEM_VALUE
,       serauth.PROV_NAME AS AuthProvider
,       serrefer.prov_name AS ReferringProv
,		serperform.PROV_NAME AS PerformingProv
,		p.PAT_MRN_ID
,		p.PAT_NAME
,		op.ORDER_PROC_ID
,        op.ORDER_TIME
,		op.PROC_ID
--,op.*
,       op.PROC_CODE
,       op.DESCRIPTION
--,ev.*
--INTO RADB.dbo.ERAS_Smartform_onepatient
FROM         Clarity.dbo.Smrtdta_Elem_Data sed               
		   JOIN   (SELECT op.*
			       from CLarity.dbo.Order_Proc op			       
			       JOIN ##erasfact f
				   ON f.pat_enc_csn_id=op.PAT_ENC_CSN_ID				   
					)op
        ON sed.record_id_numeric = op.order_proc_id
        JOIN Clarity.dbo.Smrtdta_Elem_Value ev ON sed.HLV_Id = ev.HLV_Id
        LEFT join clarity.dbo.CLARITY_CONCEPT AS cc ON sed.ELEMENT_ID = cc.CONCEPT_ID
        LEFT JOIN clarity.dbo.clarity_ser serauth ON serauth.PROV_ID = op.AUTHRZING_PROV_ID
        LEFT JOIN clarity.dbo.clarity_ser serrefer ON serrefer.PROV_ID = op.REFERRING_PROV_ID
        LEFT JOIN clarity.dbo.clarity_ser serperform ON serperform.PROV_ID = op.PROC_PERF_PROV_ID
        LEFT JOIN clarity.dbo.patient AS p          
        ON p.pat_id=op.PAT_ID
WHERE   context_name = 'ORDER'
        AND CUR_VALUE_SOURCE = 'SmartForm 11227803'
       AND ELEMENT_ID IN ( 'EPIC#12678','EPIC#19699') --including 
        --EPIC#19699 cervical,thoracic,lumbar   EPIC#12678 - block type: epidural, intrathecal, caudal
        --AND p.PAT_MRN_ID='MR86772'
        --AND ELEMENT_ID IN ( 'YNHHS#019', 'EPIC#2818', 'YNHHS#020', 'YNHHS#021' )
--AND op.ORDER_TIME>='6/1/2015'
--        AND op.ORDER_PROC_ID = 1
), t1 AS 
(SELECT CASE  
			WHEN b.PAT_ENC_CSN_ID=f.anescsn THEN 'Anescsn'
			WHEN b.PAT_ENC_CSN_ID=f.admissioncsn THEN 'Admissioncsn'
			WHEN b.PAT_ENC_CSN_ID=f.surgicalcsn THEN 'Surgicalcsn'
			ELSE '*Unknown csn'
			END AS CsnType
	,b.* 
--	,f.*
FROM basesmart b
JOIN ##erasfact f
ON b.pat_enc_csn_id=f.pat_enc_csn_id
), blocktype AS
(SELECT * 
 FROM t1
 WHERE element_id='EPIC#12678'
 ),
  spinelevel AS
(SELECT * 
 FROM t1
 WHERE element_id='EPIC#19699'
 )SELECT b.Pat_enc_csn_id,b.pat_mrn_id,b.pat_name,b.smrtdta_elem_value AS blocktype,
 s.smrtdta_elem_value AS spinelevel
 INTO ##epi
  FROM blocktype AS b
  JOIN spinelevel AS s
  ON b.pat_enc_csn_id=s.pat_enc_csn_id;
 
 --7/28/2015
--block time correct code
--pulls with anesthesia csn
IF object_id('tempdb..##blockstart') IS NOT NULL
	DROP TABLE ##blockstart;

  select eipi.PAT_CSN,eiei.event_id,eiei.line,eiei.EVENT_TYPE,eiei.EVENT_DISPLAY_NAME
 ,eiei.EVENT_TIME,eiei.EVENT_RECORD_TIME
INTO ##blockstart
 FROM clarity.dbo.F_AN_RECORD_SUMMARY fsum
  JOIN clarity.dbo.ED_IEV_PAT_INFO AS eipi
 ON fsum.AN_52_ENC_CSN_ID=eipi.PAT_CSN
  JOIN clarity.dbo.ED_IEV_EVENT_INFO AS eiei
 ON eiei.EVENT_ID=eipi.EVENT_ID 
  AND eiei.EVENT_TYPE='1120000059'
 --WHERE fsum.AN_52_ENC_CSN_ID=116823639 --prod test case
 WHERE  fsum.AN_52_ENC_CSN_ID IN (SELECT pat_enc_csn_id FROM ##epi);

--UPDATE radb.dbo.GH_ERAS_CaseFact SET blocktime=NULL,blocktype=NULL,spinelevel=null
UPDATE radb.dbo.GH_ERAS_CaseFact 
SET blocktime=epidim.blockstart
,blocktype=epidim.blocktype
,spinelevel=epidim.spinelevel
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN 
(SELECT e.pat_enc_csn_id,blocktype,spinelevel,b.EVENT_TIME AS blockstart
FROM ##epi AS e
LEFT join ##blockstart AS b
ON e.pat_enc_csn_id=b.pat_csn) epidim
ON epidim.pat_enc_csn_id=f.admissioncsn;

UPDATE radb.dbo.GH_ERAS_CaseFact 
SET blocktime=epidim.blockstart
,blocktype=epidim.blocktype
,spinelevel=epidim.spinelevel
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN 
(SELECT e.pat_enc_csn_id,blocktype,spinelevel,b.EVENT_TIME AS blockstart
FROM ##epi AS e
LEFT join ##blockstart AS b
ON e.pat_enc_csn_id=b.pat_csn) epidim
ON epidim.pat_enc_csn_id=f.anescsn;


UPDATE radb.dbo.GH_ERAS_CaseFact 
SET blocktime=epidim.blockstart
,blocktype=epidim.blocktype
,spinelevel=epidim.spinelevel
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN 
(SELECT e.pat_enc_csn_id,blocktype,spinelevel,b.EVENT_TIME AS blockstart
FROM ##epi AS e
LEFT join ##blockstart AS b
ON e.pat_enc_csn_id=b.pat_csn) epidim
ON epidim.pat_enc_csn_id=f.surgicalcsn;

UPDATE radb.dbo.GH_ERAS_CaseFact 
SET met3=1
WHERE blocktype='epidural' AND spinelevel='thoracic' 
AND ( blocktime>=anesstart AND blocktime<procedurestart);

UPDATE radb.dbo.GH_ERAS_CaseFact 
SET met3=0
WHERE met3 <>1 OR met3 IS NULL;


--metric 4. multi-modal pain management
--pre procedure

WITH baseq AS(
SELECT  CASE WHEN mai.TAKEN_TIME>=pod0_start AND mai.TAKEN_TIME<pod1_start THEN 1 ELSE 0 END AS givenpod0
,	CASE WHEN mai.TAKEN_TIME>=pod1_start AND mai.TAKEN_TIME<pod2_start THEN 1 ELSE 0 END AS givenpod1
,	CASE WHEN mai.TAKEN_TIME>=pod2_start AND mai.TAKEN_TIME<pod3_start THEN 1 ELSE 0 END AS givenpod2
,	CASE WHEN mai.TAKEN_TIME>=inpreprocedure AND mai.TAKEN_TIME<=outofpreprocedure THEN 1 ELSE 0 END AS givenpreproc
,	CASE WHEN mai.TAKEN_TIME>=hosP_admsn_time AND mai.TAKEN_TIME<sched_start_time THEN 1 ELSE 0 END AS presurg
,	CASE WHEN mai.taken_time >pod2_start THEN 1 ELSE 0 END AS pod2on
		,mai.TAKEN_TIME
		,mai.mar_action_c
		,zcact.NAME AS MarAction
		,cm.MEDICATION_ID 
		,cm.NAME AS MedicationName
  		,eb.*	
		--,om.*
--INTO  ##met4
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN ##erasfact AS eb
ON eb.pat_enc_csn_id=mai.MAR_ENC_CSN
JOIN clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
WHERE mai.MAR_ACTION_C=1
and om.MEDICATION_ID IN (SELECT MEDICATION_ID FROM radb.dbo.ERAS_Medications WHERE MetricNumber=4)
AND mai.TAKEN_TIME>=inpreprocedure AND mai.TAKEN_TIME<=COALESCE(outofpreprocedure,anesstart)


--AND mai.TAKEN_TIME>=inpreprocedure AND mai.TAKEN_TIME<=outofroom
--AND mai.TAKEN_TIME>=procedurestart AND mai.TAKEN_TIME<=procedurefinish
--AND  mai.TAKEN_TIME>=inpreprocedure AND mai.TAKEN_TIME<anesstart
)--SELECT * FROM baseq ORDER BY pat_enc_csn_id
--SELECT COUNT(*),SUM(givenpod0) AS pod0,SUM(givenpod1) AS pod1,
--SUM(givenpod2) AS pod2, SUM(givenpreproc) AS preop,
--SUM(presurg) AS presurg,SUM(pod2on) AS postpod2
--FROM(
--SELECT *
--FROM baseq
--)xx
UPDATE ##erasfact 
SET met4=1
FROM ##erasfact f
JOIN baseq b
ON f.pat_enc_csn_id=b.pat_enc_csn_id;


--SELECT * FROM clarity.dbo.zc_mar_rslt
--SELECT * FROM zc_or_status


--metric 5. normal temp on arrival to PACU
--base flowsheet query
WITH baseq AS (
SELECT  
       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,       ifm.FSD_ID
,		b.inpacu
,		b.outofpacu
,       ifm.RECORDED_TIME
,       CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) AS temp
,       ifm.MEAS_COMMENT
,		b.SURGERY_DATE
,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)

FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact AS b
        ON ids.EPT_CSN=b.pat_enc_csn_id        
        LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE          ifm.FLO_MEAS_ID IN ( '6')
        AND (ifm.RECORDED_TIME>=b.inpacu AND ifm.RECORDED_TIME< b.outofpacu)
        AND (ifm.MEAS_VALUE IS NOT NULL )
) ,temp AS 
( SELECT  PAT_ENC_CSN_ID
	FROM baseq
	WHERE rid=1
	AND temp>=96.8
	GROUP BY pat_enc_csn_id
	)	
	--SELECT * FROM temp
--SELECT * FROM baseq ORDER BY PAT_ENC_CSN_ID,RECORDED_TIME
UPDATE ##erasfact
SET met5=1
FROM ##erasfact AS f
JOIN temp AS t
ON f.pat_enc_csn_id=t.pat_enc_csn_id;
	


--LACTATED RINGERS INTRAVENOUS SOLUTION [ERX 4318]
--Inpatient
--DEXTROSE 5 % AND 0.45 % SODIUM CHLORIDE INTRAVENOUS SOLUTION [ERX 9814]
--Inpatient
--DEXTROSE 5 % AND 0.9 % SODIUM CHLORIDE INTRAVENOUS SOLUTION [ERX 9815]
--Inpatient
--SODIUM CHLORIDE 0.9 % INTRAVENOUS SOLUTION [ERX 27838]
--Inpatient

--metric 6 use of goal directed therapy

--get weight from pat enc
IF object_id('tempdb..##erasweight') IS NOT NULL
	DROP TABLE ##erasweight;

SELECT pe.WEIGHT,pe.PAT_ENC_CSN_ID,zcd.name AS Enctype
INTO ##erasweight
FROM clarity.dbo.PAT_ENC AS pe
JOIN ##erasfact f
ON pe.pat_enc_csn_id=f.pat_enc_csn_id
LEFT JOIN clarity.dbo.ZC_DISP_ENC_TYPE zcd
  ON pe.ENC_TYPE_C=zcd.DISP_ENC_TYPE_C
WHERE zcd.name ='hospital encounter'


IF object_id('tempdb..##ivintraop') IS NOT NULL
	DROP TABLE ##ivintraop;

WITH baseiv AS (
SELECT  
--b.PAT_NAME
       b.PAT_MRN_ID
--,       b.HOSP_ADMSN_TIME
--,       b.HOSP_DISCH_TIME
--,       zvt.name AS ValueType
--,       zrt.name AS RowType
,      	 ifgd.FLO_MEAS_NAME
       , ids.INPATIENT_DATA_ID
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		ifm.FLO_MEAS_ID
,       ifm.MEAS_VALUE
,		CAST(ifm.MEAS_VALUE AS NUMERIC(13,4) ) AS volume
,       ifgd.DUPLICATEABLE_YN
,		ifm.LINE
,		ifm.OCCURANCE
,		ifm.ENTRY_TIME
,       ifm.MEAS_COMMENT
,	emptaken.name AS TakenUser
,	empentry.name AS EnterByUser
,	ipx.GROUP_LINE
,	ipx.IX_FLOW_RW_ORD_ID
,	om.ORDERING_DATE
,	om.ORDER_START_TIME
,	om.ORDER_END_TIME
,	om.DISCON_TIME
,	om.ORDERING_MODE_C
,	zoc.NAME AS OrderClass
,	zos.name AS OrderStatus
,	om.DESCRIPTION AS OrderDesc
,	zar.NAME AS MedRoute
,		b.admissioncsn AS pat_enc_csn_id
,       ifm.RECORDED_TIME
,	CASE WHEN ifm.RECORDED_TIME>=b.inpreprocedure AND ifm.RECORDED_TIME<=b.outofroom
	THEN 1 ELSE 0 END AS intraopflag
,	b.case_length_hrs
,   b.inpreprocedure 
,	b.outofroom
--,		peh.INPATIENT_DATA_ID
--,		b.SURGERY_DATE
--,		b.anesstart
--,		DATEADD(hh,-3,anesstart) AS anesstart_minus3h
--,		CASE WHEN 
--		ifm.RECORDED_TIME<b.HOSP_ADMSN_TIME THEN 1 ELSE 0 
--		END AS mettime
--,		rid=ROW_NUMBER () OVER (PARTITION BY b.admissioncsn ORDER BY b.admissioncsn)
FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact b
        ON ids.EPT_CSN=b.pat_enc_csn_id
        LEFT JOIN clarity.dbo.PATIENT AS p ON b.PAT_ID = p.PAT_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
		LEFT JOIN Clarity.dbo.clarity_emp emptaken
		ON emptaken.USER_ID=ifm.TAKEN_USER_ID
		LEFT JOIN Clarity.dbo.clarity_emp empentry
		ON empentry.USER_ID=ifm.ENTRY_USER_ID
		LEFT JOIN IP_FS_ORD_IX_ID ipx 
		ON ipx.INPATIENT_DATA_ID=ifr.INPATIENT_DATA_ID
		AND ifm.OCCURANCE=ipx.GROUP_LINE
		LEFT JOIN order_med AS om
		ON om.ORDER_MED_ID=ipx.IX_FLOW_RW_ORD_ID
		--link to clarity_medication from order_med
		--if needed add filter for therapeutic or pharm class
		LEFT JOIN clarity.dbo.ZC_ORDER_CLASS AS zoc
		ON zoc.ORDER_CLASS_C=om.ORDER_CLASS_C
		LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar
		ON zar.MED_ROUTE_C=om.MED_ROUTE_C
		LEFT JOIN clarity.dbo.ZC_ORDER_STATUS AS zos
		ON zos.ORDER_STATUS_C=om.ORDER_STATUS_C
		--whERE b.pat_mrn_id ='MR9000797'
		--and ifgd.DISP_NAME LIKE '%volume%'
		--and ifm.FLO_MEAS_ID='7070009'
		where ifm.FLO_MEAS_ID='7070009'
		AND om.MEDICATION_ID   IN ('4318',--lactated ringers
								  '9814',--D5 1/2s
								  '9815',--D5 NS
								  '27838',--sodium chloride 0.9%								  
								  '9799', --GH D5 NS
								  '9801' 
								  )
), i AS 
(SELECT f.* 
,w.WEIGHT AS weight_oz
,w.weight* 0.0283495 AS weight_kg
,SUM(f.volume) OVER(PARTITION BY f.pat_enc_csn_id) AS totalvolume
,SUM(CASE WHEN f.intraopflag=1 THEN f.volume ELSE 0 END) OVER(PARTITION BY f.pat_enc_csn_id) AS totalvolume_intraop
FROM baseiv f
JOIN ##erasweight AS w
ON f.pat_enc_csn_id=w.PAT_ENC_CSN_ID
) , fin AS(
SELECT i.*
,i.weight_kg*case_length_hrs*8 AS threshold
FROM i
),firstcsn AS
(SELECT rid=ROW_NUMBER() OVER(PARTITION BY pat_enc_csn_id ORDER BY pat_enc_csn_id) 
 ,* 
 FROM fin
 )SELECT * 
  INTO ##ivintraop
  FROM firstcsn
  WHERE rid=1;

--update metric on temp table
UPDATE ##erasfact
SET met6=1
FROM ##erasfact AS f
JOIN ##ivintraop AS i
ON f.pat_enc_csn_id=i.pat_enc_csn_id
WHERE i.totalvolume_intraop<i.threshold;

--update fact table
UPDATE radb.dbo.GH_ERAS_CaseFact
SET iv_totalvolume=i.totalvolume
,iv_totalvolume_intraop=i.totalvolume_intraop
,weight_oz=i.weight_oz
,weight_kg=i.weight_kg
,IV_intraop_threshold=i.threshold
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN ##ivintraop AS i
ON i.pat_enc_csn_id=f.admissioncsn;


UPDATE radb.dbo.GH_ERAS_CaseFact
SET iv_totalvolume=i.totalvolume
,iv_totalvolume_intraop=i.totalvolume_intraop
,weight_oz=i.weight_oz
,weight_kg=i.weight_kg
,IV_intraop_threshold=i.threshold
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN ##ivintraop AS i
ON i.pat_enc_csn_id=f.surgicalcsn;

UPDATE radb.dbo.GH_ERAS_CaseFact
SET iv_totalvolume=i.totalvolume
,iv_totalvolume_intraop=i.totalvolume_intraop
,weight_oz=i.weight_oz
,weight_kg=i.weight_kg
,IV_intraop_threshold=i.threshold
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN ##ivintraop AS i
ON i.pat_enc_csn_id=f.anescsn;


----- ****** goal directed therapy end


--metric 7. multi-modal pain management
--anti-emetic - intra op
--create med list
--SELECT *
--FROM clarity.dbo.CLARITY_MEDICATION AS cm
--WHERE MEDICATION_ID IN (101,400497,24500,150333,150541,161735)
--placeholder--

WITH baseq AS (
SELECT  CASE WHEN mai.TAKEN_TIME>=pod0_start AND mai.TAKEN_TIME<pod1_start THEN 1 ELSE 0 END AS givenpod0
,	CASE WHEN mai.TAKEN_TIME>=pod1_start AND mai.TAKEN_TIME<pod2_start THEN 1 ELSE 0 END AS givenpod1
,	CASE WHEN mai.TAKEN_TIME>=pod2_start AND mai.TAKEN_TIME<pod3_start THEN 1 ELSE 0 END AS givenpod2
,	CASE WHEN mai.TAKEN_TIME>=inpreprocedure AND mai.TAKEN_TIME<=outofpreprocedure THEN 1 ELSE 0 END AS givenpreproc
		,mai.TAKEN_TIME
		,mai.SIG
		,mai.DOSE_UNIT_C
		,zmu.NAME AS DoseUnit
		,mai.mar_action_c
		,zcact.NAME AS MarAction
		,cm.MEDICATION_ID 
		,cm.NAME AS MedicationName		
  		,eb.*		
--INTO  ##met4
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN ##erasfact eb
ON eb.PAT_ENC_CSN_ID=mai.MAR_ENC_CSN
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
LEFT JOIN clarity.dbo.PATIENT AS p
ON om.PAT_ID=p.PAT_ID
WHERE mai.MAR_ACTION_C=1
and om.MEDICATION_ID IN (SELECT MEDICATION_ID FROM radb.dbo.ERAS_Medications WHERE MetricNumber=7)
and mai.TAKEN_TIME>=eb.inroom AND mai.TAKEN_TIME<eb.outofroom
)-- SELECT maraction ,COUNT(*) FROM baseq GROUP BY maraction ORDER BY COUNT(*) desc
--SELECT * FROM baseq ORDER BY pat_enc_csn_id,TAKEN_TIME 
UPDATE ##erasfact 
SET met7=1
FROM ##erasfact f
JOIN baseq b
ON b.pat_enc_csn_id=f.pat_enc_csn_id;

--Mobilization metrics:
--Metric 8 - mobilize once on POD #0
--metric 11 mobilize at least twice on POD #1  
--metric 14 mobilize at least twice on POD #2  
DECLARE    @CURRTIME AS VARCHAR(30)

SET @CURRTIME = CAST(GETDATE() AS VARCHAR) 
RAISERROR ( '%s - PRELIMS - metric 8 mobilization data' , 0, 1, @CURRTIME) WITH NOWAIT;


IF object_id('tempdb..##erasmob') IS NOT NULL
	DROP TABLE ##erasmob;


WITH baseq AS (
  		
SELECT  
       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,		b.pat_name
,		b.pat_mrn_id
,       ifm.FSD_ID
--,		b.inpacu
--,		b.outofpacu
,		b.SURGERY_DATE
,		b.SCHED_START_TIME
,		b.pod0_start
,		b.pod1_start
,		b.pod2_start
,		b.pod3_start
,       ifm.RECORDED_TIME
--,       CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) AS temp	
	,	ifm.FLO_MEAS_ID
	,	ifm.MEAS_VALUE
	,	CASE WHEN ifm.flo_meas_id='3046874' 
			THEN PATINDEX('%ambulate ad lib%',ifm.MEAS_VALUE) 
			ELSE 0 END AS adlib
	,	CASE WHEN ifm.flo_meas_id = '3046874' 
			THEN PATINDEX('%ambulate bed to/from chair%',ifm.MEAS_VALUE) 
			ELSE 0 END AS bedtochair
	,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%ambulate in hall%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS ambinhall
	,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%ambulate in room%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS ambinroom
	,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%25 ft%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS Amb25
	 ,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%50 ft%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS amb50
	 ,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%75 ft%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS amb75
	 ,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%100 ft%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS amb100
	 ,	CASE WHEN ifm.flo_meas_id = '3046874' 
	       THEN PATINDEX('%200 ft%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS amb200	       
	 --pt criteria      
     ,	CASE WHEN ifm.flo_meas_id='3047745' 
			THEN PATINDEX('%bed to chair%',ifm.MEAS_VALUE) 
			ELSE 0 END AS pt_bedtochair
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
			THEN PATINDEX('%chair to bed%',ifm.MEAS_VALUE) 
			ELSE 0 END AS pt_chairtobed
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%sidesteps%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_sidesteps
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%5 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_5ft
	       
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%10 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_10ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%15 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_15ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%20 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_20ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%25 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_25ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%50 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_50ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%75 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_75ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%100 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_100ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%150 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_150ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%200 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_200ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%250 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_250ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%300 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_300ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%350 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_350ft
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%400 feet%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_400ft	
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%x2%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_x2
	,	CASE WHEN ifm.flo_meas_id = '3047745' 
	       THEN PATINDEX('%x3%',ifm.MEAS_VALUE) 
	       ELSE 0 END AS pt_x3	

,       ifm.MEAS_COMMENT
,		totalambulate=0
,		totalpt=0
,		xmodifier=0
--,		b.SURGERY_DATE
--,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
FROM    clarity.dbo.IP_DATA_STORE AS ids
		JOIN ##erasfact AS b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID        
        LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE          ifm.FLO_MEAS_ID IN ( '3046874','3047745')
        AND (ifm.RECORDED_TIME>=b.pod0_start AND ifm.RECORDED_TIME< b.pod3_start)
        AND ifm.MEAS_VALUE IS NOT NULL 

)
SELECT * 
INTO ##erasmob
FROM baseq
WHERE (recorded_time>=pod0_start AND recorded_time <pod3_start)
ORDER BY pat_enc_csn_id,recorded_time;

--"normalize" individual events to 1 or 0
UPDATE ##erasmob
SET  adlib=	case when	adlib>0 THEN 1 ELSE 0 END,
bedtochair=	case when	bedtochair>0 THEN 1 ELSE 0 END,
ambinroom=	case when	ambinroom>0 THEN 1 ELSE 0 END,
ambinhall=	case when	ambinhall>0 THEN 1 ELSE 0 END,
amb25=	case when	amb25>0 THEN 1 ELSE 0 END,
amb50=	case when	amb50>0 THEN 1 ELSE 0 END,
amb75=	case when	amb75>0 THEN 1 ELSE 0 END,
amb100=	case when	amb100>0 THEN 1 ELSE 0 END,
amb200=	case when	amb200>0 THEN 1 ELSE 0 END,
--pt
pt_bedtochair=	case when	pt_bedtochair>0 THEN 1 ELSE 0 END,
pt_chairtobed=	case when	pt_chairtobed>0 THEN 1 ELSE 0 END,
pt_sidesteps=	case when	pt_sidesteps>0 THEN 1 ELSE 0 END,
pt_5ft=	case when	pt_5ft>0 THEN 1 ELSE 0 END,
pt_10ft=case when	pt_10ft>0 THEN 1 ELSE 0 END,
pt_15ft=case when	pt_15ft>0 THEN 1 ELSE 0 END,
pt_20ft=case when	pt_20ft>0 THEN 1 ELSE 0 END,
pt_25ft=case when	pt_25ft>0 THEN 1 ELSE 0 END,
pt_50ft=case when	pt_50ft>0 THEN 1 ELSE 0 END,
pt_75ft=case when	pt_75ft>0 THEN 1 ELSE 0 END,
pt_100ft=case when	pt_100ft>0 THEN 1 ELSE 0 END,
pt_150ft=case when	pt_150ft>0 THEN 1 ELSE 0 END,
pt_200ft=case when	pt_200ft>0 THEN 1 ELSE 0 END,
pt_250ft=case when	pt_250ft>0 THEN 1 ELSE 0 END,
pt_300ft=case when	pt_300ft>0 THEN 1 ELSE 0 END,
pt_350ft=case when	pt_350ft>0 THEN 1 ELSE 0 END,
pt_400ft=case when	pt_400ft>0 THEN 1 ELSE 0 END,
pt_x2=case when	pt_x2>0 THEN 1 ELSE 0 END,
pt_x3=case when	pt_x3>0 THEN 1 ELSE 0 END

		 	
--update total ambulation and total pt metrics
UPDATE ##erasmob
SET totalambulate =   adlib+bedtochair+ambinroom+ambinhall+amb25+amb50+amb75+amb100+amb200,
	totalpt=pt_bedtochair+pt_chairtobed+pt_sidesteps+pt_5ft+pt_10ft+pt_15ft+pt_20ft+pt_25ft+pt_50ft+pt_75ft
		   +pt_100ft+pt_150ft+pt_200ft+pt_250ft+pt_300ft+pt_350ft+pt_400ft,
		  
    xmodifier=CASE WHEN (pt_x2>0 AND pt_x3>0) AND 
			(pt_bedtochair=1 OR
			 pt_chairtobed=1 OR
			 pt_sidesteps=1	OR
			 pt_5ft=1 OR
			 pt_10ft=1	OR
			 pt_15ft=1	OR
			 pt_20ft=1	OR
			 pt_25ft=1	OR
			 pt_50ft=1	OR
			 pt_75ft=1	OR
			 pt_100ft=1	OR
			 pt_150ft=1	OR
			 pt_200ft=1	OR
			 pt_250ft=1	OR
			 pt_300ft=1	OR
			 pt_350ft=1	OR
		     pt_400ft=1) THEN 1 ELSE 0 end



--update fact table for mob pod0
UPDATE ##erasfact
SET met8=1
FROM ##erasfact AS e
JOIN (
SELECT pat_enc_csn_id
,SUM(totalambulate) AS sum_ambulate
,SUM(totalpt) AS sum_ptmob
FROM ##erasmob 
WHERE (recorded_time>=pod0_start AND recorded_time<pod1_start)
GROUP BY pat_enc_csn_id) emob
ON e.pat_enc_csn_id=emob.pat_enc_csn_id
where (sum_ambulate>0 OR sum_ptmob>0)


--update fact table for mob pod1
UPDATE ##erasfact
SET met11=1
FROM ##erasfact AS e
JOIN (
SELECT pat_enc_csn_id
,SUM(totalambulate) AS sum_ambulate
,SUM(totalpt) AS sum_ptmob
,SUM(xmodifier) AS sum_ptmodifier
FROM ##erasmob 
WHERE (recorded_time>=pod1_start AND recorded_time<pod2_start)
GROUP BY pat_enc_csn_id) emob
ON e.pat_enc_csn_id=emob.pat_enc_csn_id
where (sum_ambulate>1 OR sum_ptmob>1 OR sum_ptmodifier>=1);

--update fact table for mob pod2
--metric 14
UPDATE ##erasfact
SET met14=1
FROM ##erasfact AS e
JOIN (
SELECT pat_enc_csn_id
,SUM(totalambulate) AS sum_ambulate
,SUM(totalpt) AS sum_ptmob
,SUM(xmodifier) AS sum_ptmodifier
FROM ##erasmob 
WHERE (recorded_time>=pod2_start AND recorded_time<pod3_start)
GROUP BY pat_enc_csn_id) emob
ON e.pat_enc_csn_id=emob.pat_enc_csn_id
where (sum_ambulate>1 OR sum_ptmob>1 OR sum_ptmodifier>=1);


--metric 9 clear liquids POD#0
WITH baseq AS (
SELECT  
       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,       ifm.FSD_ID
,		b.inpacu
,		b.outofpacu
,		ifm.flo_meas_id
,       ifm.RECORDED_TIME
,		ifm.MEAS_VALUE
,       CASE WHEN ISNUMERIC(ifm.meas_value)=1 then CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) ELSE 0 END AS amt
,       ifm.MEAS_COMMENT
,		b.SURGERY_DATE
,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
FROM    clarity.dbo.IP_DATA_STORE AS ids
		JOIN ##erasfact AS b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID        
        JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE          ifm.FLO_MEAS_ID IN ( '51','604258654')
        AND (ifm.RECORDED_TIME>=b.pod0_start AND ifm.RECORDED_TIME< b.pod1_start)
        AND (ifm.MEAS_VALUE IS NOT NULL )
) --SELECT * FROM baseq b JOIN ##erasfact f ON b.pat_enc_csn_id=f.pat_enc_csn_id ORDER BY b.pat_enc_csn_id,recorded_time
UPDATE ##erasfact 
SET met9=1
FROM ##erasfact f
JOIN (
	SELECT  PAT_ENC_CSN_ID
	FROM baseq
	WHERE amt>0
	GROUP BY pat_enc_csn_id
	) fluid ON f.pat_enc_csn_id=fluid.pat_enc_csn_id;






--metric 10. IV fluids discontinued within POD#0 window

WITH baseq AS (
SELECT  CASE WHEN mai.TAKEN_TIME>=pod0_start AND mai.TAKEN_TIME<pod1_start THEN 1 ELSE 0 END AS givenpod0
,	CASE WHEN mai.TAKEN_TIME>=pod1_start AND mai.TAKEN_TIME<pod2_start THEN 1 ELSE 0 END AS givenpod1
,	CASE WHEN mai.TAKEN_TIME>=pod2_start AND mai.TAKEN_TIME<pod3_start THEN 1 ELSE 0 END AS givenpod2
,	CASE WHEN mai.TAKEN_TIME>=eb.inroom AND mai.TAKEN_TIME<eb.outofroom THEN 1 ELSE 0 END AS givenintra
,mai.TAKEN_TIME
,mai.MAR_ACTION_C
,zcmar.name AS maraction
,cm.NAME AS medname
		,eb.*		
		,om.ORDER_MED_ID
		,om.MEDICATION_ID
		,om.order_class_C
		,om.ORDER_START_TIME
		,om.ORDER_END_TIME
		,om.End_date
		,zoc.name AS OrderClass
		--,om.*
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN ##erasfact eb
ON eb.PAT_ENC_CSN_ID=mai.MAR_ENC_CSN
JOIN clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.CLARITY_MEDICATION AS cm
ON cm.MEDICATION_ID=om.MEDICATION_ID
LEFT JOIN ZC_MAR_RSLT AS zcmar
ON zcmar.RESULT_C=mai.MAR_ACTION_C
LEFT JOIN clarity.dbo.ZC_ORDER_CLASS AS zoc
ON zoc.ORDER_CLASS_C=om.ORDER_CLASS_C
where om.MEDICATION_ID IN ('4318',--lactated ringers
						   '9814',--D5 1/2s
						   '9815',--D5 NS
						   '27838',--sodium chloride 0.9%								  						   
						    '9801')
and mai.MAR_ACTION_C =8 --stopped
) 
--SELECT b.pat_mrn_id,givenpod0,givenpod1,givenpod2,givenintra,maraction,medication_id,medname,pat_enc_csn_id,taken_time,hosp_admsn_time,hosp_disch_time,sched_start_time,pod0_start,pod1_start,pod2_start,inroom,outofroom
--FROM baseq  b
--ORDER BY pat_enc_csn_id,taken_time
UPDATE ##erasfact
SET met10=1
FROM ##erasfact f
JOIN baseq b
ON f.pat_enc_csn_id=b.pat_enc_csn_id
WHERE b.givenpod0=1;


--metric 11
---** completed earlier in code **


--metric 12 --solid food given POD 1
IF object_id('tempdb..##met12diet') IS NOT NULL
	DROP TABLE ##met12diet;

SELECT f.*    
,CASE WHEN OP_CHLD.ORDER_TIME>=pod0_start AND OP_CHLD.ORDER_TIME<pod1_start THEN 1 ELSE 0 END AS orderpod0
,	CASE WHEN OP_CHLD.ORDER_TIME>=pod1_start AND OP_CHLD.ORDER_TIME<pod2_start THEN 1 ELSE 0 END AS orderpod1
,	CASE WHEN OP_CHLD.ORDER_TIME>=pod2_start AND OP_CHLD.ORDER_TIME<pod3_start THEN 1 ELSE 0 END AS orderpod2
,	CASE WHEN OP_CHLD.ORDER_TIME>=f.inroom AND OP_CHLD.ORDER_TIME<f.outofroom THEN 1 ELSE 0 END AS orderintra
      , OP_PRNT.ORDER_PROC_ID AS PRNT_ORDER_PROC_ID
       , OP_PRNT.PROC_ID AS PRNT_PROC_ID
       , op_prnt.REASON_FOR_CANC_C AS PRNT_REASON_FOR_CANC_C
       , PRNT_Cancel.NAME AS ParentCancelReason
       , OP_CHLD.REASON_FOR_CANC_C AS CHLD_REASON_FOR_CANC_C
       , CHLD_Cancel.NAME AS ChildCancelReason
       , EAP.PROC_NAME AS PRNT_PROC_NAME
       , OP_PRNT.ORDER_STATUS_C AS PRNT_ORDER_STATUS_C
       , PRNT_OS.NAME AS PRNT_ORDER_STATUS
       , OP_PRNT.FUTURE_OR_STAND
       , OP_PRNT.INSTANTIATED_TIME
       , OP_PRNT.IS_PENDING_ORD_YN
       , OP_PRNT.STANDING_OCCURS
       , OP_PRNT.STAND_ORIG_OCCUR
       , OP_PRNT.ORDERING_DATE
       , OP_CHLD.ORDER_PROC_ID AS CHLD_ORDER_PROC_ID
       , OP_CHLD.PROC_ID AS CHLD_PROC_ID
       , CEAP.PROC_NAME AS CHLD_PROC_NAME
       , OP_CHLD.ORDER_STATUS_C AS CHLD_ORDER_STATUS_C
       , CHLD_LAB_STAT.NAME AS CHLD_LAB_STATUS
       , CHLD_OS.NAME AS CHLD_ORDER_STATUS
       , OP_CHLD.ORDER_TIME AS CHLD_ORDER_TIME 
       , OP_CHLD.PROC_START_TIME AS CHLD_START_TIME
       , OP_CHLD.RESULT_TIME AS CHLD_RESULT_TIME
       , CHLD_TURN_AROUND_TIME = CAST((DATEDIFF(MINUTE,  OP_CHLD.ORDER_TIME,  OP_CHLD.RESULT_TIME)) AS NUMERIC(20,2))
       --, CHLD_TURN_AROUND_TIME_FIRST_RESULT = CAST((DATEDIFF(MINUTE,  OP_CHLD.ORDER_TIME,  OSTAT.ROUTING_INST_TM)) AS NUMERIC(20,2))
INTO ##met12diet
FROM Clarity.dbo.ORDER_PROC AS OP_PRNT WITH(NOLOCK)
INNER JOIN Clarity.dbo.ORDER_INSTANTIATED AS OI WITH(NOLOCK)
     ON OP_PRNT.ORDER_PROC_ID = OI.ORDER_ID
INNER JOIN Clarity.dbo.ORDER_PROC AS OP_CHLD WITH(NOLOCK)
     ON OI.INSTNTD_ORDER_ID = OP_CHLD.ORDER_PROC_ID

INNER JOIN Clarity.dbo.PAT_ENC_HSP AS PEH WITH(NOLOCK) 
     ON PEH.PAT_ENC_CSN_ID = OP_PRNT.PAT_ENC_CSN_ID

INNER JOIN ##erasfact f
ON f.pat_enc_csn_id=peh.PAT_ENC_CSN_ID

LEFT JOIN Clarity.dbo.ZC_CONF_STAT AS CNF_STAT WITH(NOLOCK)
     ON PEH.ADMIT_CONF_STAT_C = CNF_STAT.ADMIT_CONF_STAT_C
LEFT JOIN Clarity.dbo.ZC_ORDER_STATUS AS CHLD_OS WITH(NOLOCK)
     ON OP_CHLD.ORDER_STATUS_C = CHLD_OS.ORDER_STATUS_C
LEFT JOIN Clarity.dbo.ZC_ORDER_STATUS AS PRNT_OS WITH(NOLOCK)
     ON OP_PRNT.ORDER_STATUS_C = PRNT_OS.ORDER_STATUS_C
LEFT JOIN Clarity.dbo.CLARITY_EAP AS EAP WITH(NOLOCK)
     ON OP_PRNT.PROC_ID = EAP.PROC_ID
LEFT JOIN Clarity.dbo.CLARITY_EAP AS CEAP WITH(NOLOCK)
     ON OP_CHLD.PROC_ID = CEAP.PROC_ID
LEFT JOIN Clarity.dbo.ZC_LAB_STATUS AS CHLD_LAB_STAT
     ON OP_CHLD.LAB_STATUS_C = CHLD_LAB_STAT.LAB_STATUS_C
LEFT JOIN clarity.dbo.ZC_REASON_FOR_CANC AS PRNT_Cancel
ON   PRNT_Cancel.REASON_FOR_CANC_C=OP_PRNT.REASON_FOR_CANC_C
LEFT JOIN clarity.dbo.ZC_REASON_FOR_CANC AS CHLD_Cancel
ON   CHLD_Cancel.REASON_FOR_CANC_C=OP_CHLD.REASON_FOR_CANC_C

WHERE (ISNULL(CHLD_OS.NAME, 'Active') <> 'Canceled'     
       OR  OP_CHLD.ORDER_STATUS_C=4 AND OP_CHLD.REASON_FOR_CANC_C=14)
       AND OP_PRNT.IS_PENDING_ORD_YN = 'N'
       AND OP_CHLD.IS_PENDING_ORD_YN = 'N'
       AND OP_CHLD.proc_id IN (40485,40487,40493,40499,40501,40503,40505,40517,40519,40521,40533,40535,
							  40537,40539,40541,40553,40555,40557,40565,40567,40573,40581,40585,40587,40593,63550,80119,
							  80129,80135,80139,80143,80145,80151,80153,80163,80169,80175,89784,89794,89796,97659,99068,99111,
							  99762,99763,99776,100111,101576,30415516326,111222555888,7988564596874)
       AND OP_CHLD.PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM ##ERASFACT)
       AND (OP_CHLD.PROC_START_TIME>=f.pod1_2pm AND OP_CHLD.PROC_START_TIME<f.pod2_start) --order is placed between 2PM or greater POD 1
       ORDER BY OP_CHLD.PAT_ENC_CSN_ID,OP_CHLD.ORDERING_DATE;									 --and before Midnight POD 2

--SELECT * FROM ##met12diet AS m select * from ##erasfact

--metric 12 step 2: check flowsheet records
WITH baseq AS (
SELECT  
       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,		b.pat_mrn_id
,       ifm.FSD_ID
,	    ifm.FLO_MEAS_ID
,		b.pod1_start
,		b.pod2_start
,		b.pod1_2pm
,       ifm.RECORDED_TIME
,		ifm.MEAS_VALUE
,       CASE WHEN ISNUMERIC(ifm.meas_value)=1 then CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) ELSE 0 END AS amt
,       ifm.MEAS_COMMENT
,	CASE WHEN ifm.flo_meas_id='4515' 
			THEN PATINDEX('%fair%',ifm.MEAS_VALUE) 
			ELSE 0 END AS fair
,	CASE WHEN ifm.flo_meas_id='4515' 
			THEN PATINDEX('%good%',ifm.MEAS_VALUE) 
			ELSE 0 END AS good			
,		b.SURGERY_DATE
,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID
        JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE ifm.FLO_MEAS_ID IN ( '5966','4515') --5966 Intake %, 4515 Diet Feeding/Tolerance
        --AND (ifm.RECORDED_TIME>= DATEADD(hh,12,b.pod1_start) AND ifm.RECORDED_TIME< b.pod2_start)
        AND (ifm.MEAS_VALUE IS NOT NULL )
        
), fin AS 
( SELECT disp_name,pat_mrn_id,pat_enc_csn_id,pod1_2pm,pod1_start,pod2_start,recorded_time,flo_meas_id,meas_value,ValueType,RowType 
  FROM baseq
  WHERE 
		(  (flo_meas_id='5966' AND MEAS_VALUE IN ('50%','75%','100%'))
           OR  (flo_meas_id='4515' AND (fair>0 OR good>0))
		)        
       AND (RECORDED_TIME>=pod1_2pm AND RECORDED_TIME< pod2_start)        
), tolerance AS
( SELECT pat_enc_csn_id
  FROM fin
  WHERE flo_meas_id='4515'
  GROUP BY pat_enc_csn_id
  HAVING COUNT(*)>=1
  )
  , intake AS
( SELECT pat_enc_csn_id
  FROM fin
  WHERE flo_meas_id='5966'
  GROUP BY pat_enc_csn_id
  HAVING COUNT(*)>=1
  ) 
--  SELECT * FROM fin
--SELECT f.* 
--FROM fin AS f
--JOIN intake AS i
--ON f.pat_enc_csn_id=i.pat_enc_csn_id
--JOIN tolerance t
--ON t.pat_enc_csn_id=i.pat_enc_csn_id
-- ORDER BY pat_enc_csn_id,flo_meas_id,recorded_time
UPDATE ##erasfact
SET met12flow=1
FROM ##erasfact f
JOIN (
SELECT DISTINCT f.pat_enc_csn_id 
FROM fin AS f
JOIN intake AS i
ON f.pat_enc_csn_id=i.pat_enc_csn_id
JOIN tolerance t
ON t.pat_enc_csn_id=i.pat_enc_csn_id
) met12pass ON f.pat_enc_csn_id=met12pass.pat_enc_csn_id;

--update for diet orders
UPDATE ##erasfact
SET met12orders=1
FROM ##erasfact f
JOIN ##met12diet d
ON f.pat_enc_csn_id=d.pat_enc_csn_id


UPDATE ##erasfact
SET met12=1
WHERE met12orders=1 AND met12flow=1;


--metric 13 removal of foley on POD #0 or POD#1
--need to pull list of foley's based on a csn population

--SELECT FLO_MEAS_ID,FLO_MEAS_NAME,DISP_NAME,ifgd.ROW_TYP_C,DUPLICATEABLE_YN,rowtype=zrt.NAME
--FROM dbo.IP_FLO_GP_DATA AS ifgd
--LEFT JOIN dbo.ZC_ROW_TYP AS zrt
--ON ifgd.ROW_TYP_C=zrt.ROW_TYP_C
--WHERE FLO_MEAS_ID IN ( '3048148000', '8148', '8151' )


WITH baseq AS (
SELECT  p.PAT_NAME
,       p.PAT_MRN_ID
,       f.hosp_admsn_time
,       f.surgery_date
,       f.sched_start_time
,       f.pod0_start
,       f.pod1_start
,       f.pod2_start
,       iln.PAT_ENC_CSN_ID
,       iln.IP_LDA_ID
,       iln.PLACEMENT_INSTANT
,		CASE WHEN iln.REMOVAL_INSTANT>=pod0_start AND iln.REMOVAL_INSTANT<pod2_start THEN 1 ELSE 0 END AS removalflag
,       iln.REMOVAL_INSTANT
,       iln.DESCRIPTION
,       iln.PROPERTIES_DISPLAY
,		iln.FSD_ID
,		ifgd.DUPLICATEABLE_YN
,		ifgd.FLO_MEAS_NAME
,		ifgd.DISP_NAME
,		rowtype=zrt.name
FROM    ##erasfact f
		JOIN dbo.IP_DATA_STORE AS ids
		ON ids.EPT_CSN=f.pat_enc_csn_id
		JOIN dbo.IP_LDA_INPS_USED AS iliu
		ON ids.INPATIENT_DATA_ID=iliu.INP_ID
		JOIN dbo.IP_LDA_NOADDSINGLE AS iln 
				ON iln.IP_LDA_ID=iliu.IP_LDA_ID				        
		LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd
		ON iln.FLO_MEAS_ID=ifgd.FLO_MEAS_ID				
		LEFT JOIN dbo.ZC_ROW_TYP AS zrt
		ON ifgd.ROW_TYP_C=zrt.ROW_TYP_C
       LEFT JOIN pat_enc_hsp AS peh ON f.pat_enc_csn_id = peh.PAT_ENC_CSN_ID
       JOIN dbo.PATIENT AS p ON peh.pat_id = p.PAT_ID
WHERE   iln.FLO_MEAS_ID IN ( '3048148000', '8148', '8151' )
--WHERE iln.FLO_MEAS_ID IN ('8116')
), fin AS (
SELECT pat_enc_csn_id 
FROM baseq
GROUP BY pat_enc_csn_id
HAVING SUM(removalflag)>0
)-- SELECT * FROM baseq
UPDATE ##erasfact
SET met13=1
FROM ##erasfact f
JOIN fin x ON f.pat_enc_csn_id=x.pat_enc_csn_id;

--metric14
--completed earlier

--metric 15 return of bowel function


--SET @CURRTIME = CAST(GETDATE() AS VARCHAR) ;
--RAISERROR ( '%s - PRELIMS - metric 15 bowel function' , 0, 1, @CURRTIME) WITH NOWAIT;

WITH baseq AS (
SELECT  
      
       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,		b.PAT_ENC_CSN_ID
,		b.pat_mrn_id
,       ifm.FSD_ID
--,		b.inpacu
--,		b.outofpacu
,		b.SURGERY_DATE
,		b.SCHED_START_TIME
,		B.outofroom
,		b.pod0_start
,		b.pod1_start
,		b.pod2_start
,		b.pod3_start
,       ifm.RECORDED_TIME
--,       CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) AS temp
	,	ifm.FLO_MEAS_ID
	,	ifm.MEAS_VALUE
	,	CASE WHEN ifm.flo_meas_id='5202' THEN
			  DATEADD(dd,CONVERT(int,ifm.meas_value),'12/31/1840') END AS lastboweldate			  
	,	CASE WHEN ifm.flo_meas_id='4423' 
			THEN PATINDEX('%passing flatus%',ifm.MEAS_VALUE) 
			ELSE 0 END AS flatus
	,	CASE WHEN ifm.flo_meas_id = '304340' 
			THEN CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) 
			END AS stooloccurrence
	,	CASE WHEN ifm.flo_meas_id = '305020' 
	       THEN CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) 
			END AS stool	
	,	CASE WHEN ifm.flo_meas_id = '661980' 
	       THEN CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) 
			END AS stooloutput
	,	CASE WHEN ifm.flo_meas_id='664202' 
			THEN PATINDEX('%flatus%',ifm.MEAS_VALUE) 
			ELSE 0 END AS stomaflatus				
				
,       ifm.MEAS_COMMENT
--,		b.SURGERY_DATE
--,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
FROM    clarity.dbo.IP_DATA_STORE AS ids
		JOIN ##erasfact AS b ON ids.EPT_CSN = b.PAT_ENC_CSN_ID                		        
        LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE ifm.FLO_MEAS_ID IN ( '5202','4423','304340','305020','661980','664202')
        --AND (ifm.RECORDED_TIME>=b.outofroom AND ifm.RECORDED_TIME< b.hosp_disch_time) --prod
        --AND ifm.RECORDED_TIME>=b.outofroom --test
        AND ifm.MEAS_VALUE IS NOT NULL 
  		

), fin AS (
SELECT CASE WHEN 
		ISDATE(lastboweldate)=1
		OR flatus>0
		OR stooloccurrence>0
		OR stool>0
		OR stooloutput>0
		OR stomaflatus>0		
		THEN 1 ELSE 0 END
		AS bowelfunction

,*
FROM baseq
) , csnlist AS (
  SELECT pat_enc_csn_id ,MIN(recorded_time) AS DateBowelFunction
  FROM fin
  WHERE bowelfunction=1
  GROUP BY pat_enc_csn_id
  )--SELECT * FROM baseq
  --SELECT COUNT(*),COUNT(DISTINCT pat_enc_cSN_id) FROM csnlist ORDER BY pat_enc_csn_id,recorded_time
   UPDATE ##erasfact
   SET met15=1,met15date=csn.DateBowelFunction
   FROM ##erasfact f
   JOIN csnlist csn
   ON f.pat_enc_csn_id=csn.pat_enc_csn_id;
   
   
 
 
  


--metric 16 Date tolerating diet
--DECLARE    @CURRTIME AS VARCHAR(30)
--SET @CURRTIME = CAST(GETDATE() AS VARCHAR) 
--RAISERROR ( '%s - PRELIMS - metric 8 mobilization data' , 0, 1, @CURRTIME) WITH NOWAIT;


WITH baseq AS (
SELECT  
CASE WHEN ifm.recorded_time>=pod0_start AND ifm.recorded_time<pod1_start THEN 1 ELSE 0 END AS recordedpod0
,	CASE WHEN ifm.recorded_time>=pod1_start AND ifm.recorded_time<pod2_start THEN 1 ELSE 0 END AS recordedpod1
,	CASE WHEN ifm.recorded_time>=pod2_start AND ifm.recorded_time<pod3_start THEN 1 ELSE 0 END AS recordedpod2
,	CASE WHEN ifm.recorded_time>=b.inroom AND ifm.recorded_time<b.outofroom THEN 1 ELSE 0 END AS recordedintra
,	CASE WHEN (ifm.RECORDED_TIME>=b.outofroom AND ifm.RECORDED_TIME< b.hosp_disch_time) THEN 1 ELSE 0 END AS recordedpostop
,       ifgd.DUPLICATEABLE_YN
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		b.pat_enc_csn_id
,		b.inpacu
,		b.outofpacu
,		b.SURGERY_DATE
,		b.SCHED_START_TIME
,		B.outofroom
,		b.pod0_start
,		b.pod1_start
,		b.pod2_start
,		b.pod3_start
,       ifm.RECORDED_TIME
--,       CONVERT(NUMERIC(13,4),ifm.MEAS_VALUE) AS temp	
	,	ifm.FLO_MEAS_ID
	,	ifm.MEAS_VALUE
	,	CASE WHEN ifm.flo_meas_id='5966' THEN
			CASE WHEN   PATINDEX('%[%]%',ifm.MEAS_VALUE) >0 THEN
				CONVERT(int,SUBSTRING(ifm.meas_value,1,PATINDEX('%[%]%',ifm.MEAS_VALUE)-1))
			ELSE null END 
				END AS intakepct	
	,	CASE WHEN ifm.flo_meas_id='4515' 
			THEN PATINDEX('%fair%',ifm.MEAS_VALUE) 
			ELSE 0 END AS tolerancefair
	,	CASE WHEN ifm.flo_meas_id='4515' 
			THEN PATINDEX('%good%',ifm.MEAS_VALUE) 
			ELSE 0 END AS tolerancegood			
,       ifm.MEAS_COMMENT
--,		b.SURGERY_DATE
--,		rid=ROW_NUMBER () OVER (PARTITION BY b.PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN ##erasfact b   ON ids.EPT_CSN=b.pat_enc_csn_id
        LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
        WHERE ifm.FLO_MEAS_ID IN ( '5966','4515')
        AND (ifm.RECORDED_TIME>=b.outofroom AND ifm.RECORDED_TIME< b.hosp_disch_time) --prod
        
        AND ifm.MEAS_VALUE IS NOT NULL  		 
), pct AS (
SELECT pat_enc_csn_id,RECORDED_TIME
FROM baseq
WHERE FLO_MEAS_ID='5966'
AND intakepct>=50
AND recordedpostop=1
),tolerance AS (
SELECT pat_enc_csn_id,RECORDED_TIME
FROM baseq
WHERE FLO_MEAS_ID='4515'
and (tolerancefair=1 or tolerancegood=1)
AND recordedpostop=1
), almostfinallist AS (
 SELECT p.pat_enc_csn_id,MIN(p.RECORDED_TIME) AS DatePct,MIN(t.RECORDED_TIME) AS DateTolerated
  FROM pct AS p
  JOIN tolerance AS t
  ON p.pat_enc_csn_id=t.pat_enc_csn_id
  GROUP BY p.pat_enc_csn_id
  ) ,finallist AS (
  SELECT pat_enc_csn_id
  ,DatePct
  ,DateTolerated
  ,DateFirstTolerated=
		CASE WHEN DatePct<=DateTolerated THEN DatePct ELSE DateTolerated END
  FROM almostfinallist
  )  --SELECT * FROM finallist
UPDATE ##erasfact
SET met16=1,met16date=csn.DateFirstTolerated
FROM ##erasfact f
JOIN  finallist csn
ON f.pat_enc_csn_id=csn.pat_enc_csn_id;



--metric 17 


IF object_id('tempdb..#met17') IS NOT NULL
DROP TABLE #met17;

WITH baseq AS (
SELECT  CASE WHEN mai.TAKEN_TIME>=pod0_start AND mai.TAKEN_TIME<pod1_start THEN 1 ELSE 0 END AS givenpod0
,	CASE WHEN mai.TAKEN_TIME>=pod1_start AND mai.TAKEN_TIME<pod2_start THEN 1 ELSE 0 END AS givenpod1
,	CASE WHEN mai.TAKEN_TIME>=pod2_start AND mai.TAKEN_TIME<pod3_start THEN 1 ELSE 0 END AS givenpod2
,	CASE WHEN mai.TAKEN_TIME>=eb.outofroom AND mai.TAKEN_TIME<=eb.hosp_disch_time THEN 1 ELSE 0 END AS givenpostop
		,mai.TAKEN_TIME
		,mai.SIG
		,mai.DOSE_UNIT_C
		,zmu.NAME AS DoseUnit
		,mai.mar_action_c
		,zcact.NAME AS MarAction
		,cm.MEDICATION_ID 
		,cm.NAME AS MedicationName		
  		,eb.*		
  		,rid=ROW_NUMBER () OVER(PARTITION BY log_id ORDER BY log_id)
--INTO  ##met4
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN ##erasfact eb
ON eb.PAT_ENC_CSN_ID=mai.MAR_ENC_CSN
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
WHERE mai.MAR_ACTION_C=8 --stopped
and om.MEDICATION_ID IN (SELECT MEDICATION_ID FROM radb.dbo.ERAS_Medications WHERE MetricNumber=17)

AND mai.TAKEN_TIME>=outofroom AND mai.TAKEN_TIME<=hosp_disch_time --prod
), final AS (
SELECT pat_enc_csn_id
,MAX(taken_time) AS LastMARStopTime
FROM baseq
WHERE givenpostop=1
GROUP BY pat_enc_csn_id
)
--SELECT * 
--FROM ##erasfact f
--JOIN baseq m
--ON f.pat_enc_csn_id=m.pat_enc_csn_id;

UPDATE ##erasfact
SET met17date=m.LastMARStopTime,met17=1
FROM ##erasfact f
JOIN final m
ON f.pat_enc_csn_id=m.pat_enc_csn_id;

--SELECT *
--FROM dbo.ORDER_MED AS om
--JOIN ##erasfact f
--ON om.PAT_ENC_CSN_ID=f.pat_enc_csn_id


--SELECT * 
-- INTO #met17
-- FROM final;

--UPDATE ##erasfact
--SET met17date=m.LastMARStopTime
--FROM ##erasfact f
--JOIN #met17 m
--ON f.pat_enc_csn_id=m.pat_enc_csn_id

--UPDATE ##erasfact
--SET met17=1
--FROM ##erasfact f
--JOIN #met17 m
--ON f.pat_enc_csn_id=m.pat_enc_csn_id


--SELECT * FROM radb.dbo.GH_ERAS_CaseFact 


UPDATE radb.dbo.GH_ERAS_CaseFact 
SET met1= CASE WHEN tmp.met1>0 THEN 1 ELSE 0 end,
met2= CASE WHEN tmp.met2>0 THEN 1 ELSE 0 end,
--met3= CASE WHEN tmp.met3>0 THEN 1 ELSE 0 end,
met4= CASE WHEN tmp.met4>0 THEN 1 ELSE 0 end,
met5= CASE WHEN tmp.met5>0 THEN 1 ELSE 0 end,
met6= CASE WHEN tmp.met6>0 THEN 1 ELSE 0 end,
met7= CASE WHEN tmp.met7>0 THEN 1 ELSE 0 end,
met8= CASE WHEN tmp.met8>0 THEN 1 ELSE 0 end,
met9= CASE WHEN tmp.met9>0 THEN 1 ELSE 0 end,
met10= CASE WHEN tmp.met10>0 THEN 1 ELSE 0 end,
met11= CASE WHEN tmp.met11>0 THEN 1 ELSE 0 end,
met12= CASE WHEN tmp.met12>0 THEN 1 ELSE 0 end,
met12orders= CASE WHEN tmp.met12orders>0 THEN 1 ELSE 0 end,
met12flow= CASE WHEN tmp.met12flow>0 THEN 1 ELSE 0 end,
met13= CASE WHEN tmp.met13>0 THEN 1 ELSE 0 end,
met14= CASE WHEN tmp.met14>0 THEN 1 ELSE 0 end,
met15= CASE WHEN tmp.met15>0 THEN 1 ELSE 0 end,
met15date=tmp.met15date,
met16= CASE WHEN tmp.met16>0 THEN 1 ELSE 0 end,
met16date=tmp.met16date,
met17= CASE WHEN tmp.met17>0 THEN 1 ELSE 0 END,
met17date=tmp.met17date
FROM radb.dbo.GH_ERAS_CaseFact f
JOIN   
 (SELECT log_id,
SUM(met1) AS met1,
SUM(met2) AS met2,
SUM(met3) AS met3,
SUM(met4) AS met4,
SUM(met5) AS met5,
SUM(met6) AS met6,
SUM(met7) AS met7,
SUM(met8) AS met8,
SUM(met9) AS met9,
SUM(met10) AS met10,
SUM(met11) AS met11,
SUM(met12) AS met12,
SUM(met12flow) AS met12flow,
SUM(met12orders) AS met12orders,
SUM(met13) AS met13,
SUM(met14) AS met14,
SUM(met15) AS met15,
MIN(met15date) AS met15date,
SUM(met16) AS met16,
MIN(met16date) AS met16date,
SUM(met17) AS met17,
MIN(met17date) AS met17date
FROM ##erasfact
GROUP BY log_id
)	 tmp ON f.LOG_ID=tmp.log_id


