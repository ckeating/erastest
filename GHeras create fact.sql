


--Roll out priority 1,2,5,8,9,11,12,14,16 first.
--done:
--1: pre admission counseling
--2: clear liquids up to 3 hours before induction
--5: Normal temp on PACU arrival
--8: ambulation pod0
--9: clear liquids given POD0
--11: ambulation pod1
--14: ambulation pod2

	--SELECT * 
	--FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail
	--WHERE  FLO_MEAS_ID IN ('1020100004','1217')
	--ORDER BY PAT_ENC_CSN_ID




IF object_id('RADB.dbo.CRD_ERAS_ghgi_FlowDetail') IS NOT NULL
	DROP TABLE RADB.dbo.CRD_ERAS_ghgi_FlowDetail;

WITH baseq AS (
  		
SELECT  
		b.LOG_ID
,		b.admissioncsn AS pat_enc_csn_id
,       ifm.FSD_ID
,		ifm.line
,       ifgd.FLO_MEAS_NAME
,		ifm.FLO_MEAS_ID
,       ifgd.DISP_NAME AS Flowsheet_DisplayName
,		ifm.MEAS_VALUE
,		MEAS_NUMERIC=CAST(NULL AS NUMERIC(13,4))
,		MEAS_DATE = cast (NULL AS DATETIME)
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
,		IP_LDA_ID =CAST(NULL AS VARCHAR(18))
,		LDAName	=CAST(NULL AS VARCHAR(254))
,	    PLACEMENT_DTTM	=CAST(NULL AS datetime)
,		REMOVAL_DTTM =CAST(NULL AS DATETIME)
,		LDA_Description = CAST(NULL AS VARCHAR(254))
,		LDA_Properties = CAST(NULL AS VARCHAR(254))
,		admissioncsnflag=CAST(NULL AS INT)
,		anesthesiacsnflag=CAST(NULL AS INT)
,		b.pat_name
,		b.pat_mrn_id
	--ambulation criteria
	,	amb_adlib=CAST(NULL AS TINYINT	)
	,	amb_bedtfchair=CAST(NULL AS TINYINT)
	,	amb_inroom=CAST(NULL AS TINYINT)
	,	amb_inhall=CAST(NULL AS TINYINT)	       
	,	amb_25ft=CAST(NULL AS TINYINT)
	,	amb_50ft=CAST(NULL AS TINYINT)
	,	amb_75ft=CAST(NULL AS TINYINT)
	,	amb_100ft=CAST(NULL AS TINYINT)
	,	amb_200ft=CAST(NULL AS TINYINT)
	
	--pt criteria      
     ,	pt_bedtochair=CAST(NULL AS TINYINT)
	,	pt_chairtobed=CAST(NULL AS TINYINT)
	,	pt_sidesteps=CAST(NULL AS TINYINT)
	,	pt_5ft=CAST(NULL AS TINYINT)	       
	,	pt_10ft=CAST(NULL AS TINYINT)
	,	pt_15ft=CAST(NULL AS TINYINT)
	,	pt_20ft=CAST(NULL AS TINYINT)
	,	pt_25ft=CAST(NULL AS TINYINT)
	,	pt_50ft=CAST(NULL AS TINYINT)
	,	pt_75ft=CAST(NULL AS TINYINT)
	,	pt_100ft=CAST(NULL AS TINYINT)
	,	pt_150ft=CAST(NULL AS TINYINT)
	,	pt_200ft=CAST(NULL AS TINYINT)
	,	pt_250ft=CAST(NULL AS TINYINT)
	,	pt_300ft=CAST(NULL AS TINYINT)
	,	pt_350ft=CAST(NULL AS TINYINT)
	,	pt_400ft=CAST(NULL AS TINYINT)
	,	pt_x2=CAST(NULL AS TINYINT)
	,	pt_x3	=CAST(NULL AS TINYINT)
	,	ambulate_num=CAST(NULL AS TINYINT)
	,	ambulate_den=CAST(NULL AS TINYINT)
	,	preadmit_tm=CAST(NULL AS TINYINT)
	,	anes_minus3=CAST(NULL AS TINYINT)
	,	anes_minus2=CAST(NULL AS TINYINT)
	,	presurg=CAST(NULL AS TINYINT)
	,   preop=CAST(NULL AS TINYINT)
	,   intraop=CAST(NULL AS TINYINT)
	,   pacu=CAST(NULL AS TINYINT)
	,   postop0=CAST(NULL AS TINYINT)	
	,	postop0_6pm=CAST(NULL AS TINYINT)	
	,	postopday1=CAST(NULL AS TINYINT)
	,	postopday2=CAST(NULL AS TINYINT)
	,	postopday3=CAST(NULL AS TINYINT)
	,	afterpostopday4=CAST(NULL AS TINYINT)
	,   Postop_disch=CAST(NULL AS TINYINT)
	,   PhaseofCare_id=CAST(NULL AS TINYINT)
	,	PhaseofCare_desc= CAST(NULL AS VARCHAR(25))
	,   ProcEnd=COALESCE(b.procedurefinish,b.outofroom)
	,	DischargeTime=b.HOSP_DISCH_TIME

FROM    clarity.dbo.IP_DATA_STORE AS ids
		--clarity.dbo.pat_enc_hsp AS ids
		JOIN radb.dbo.crd_eras_ghgi_case   b ON ids.EPT_CSN = b.admissioncsn
        LEFT JOIN clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
		LEFT JOIN clarity.dbo.CLARITY_EMP AS emptaken ON emptaken.USER_ID=ifm.TAKEN_USER_ID
		LEFT JOIN clarity.dbo.CLARITY_EMP AS empent ON empent.USER_ID=ifm.ENTRY_USER_ID
		
        WHERE          ifm.FLO_MEAS_ID IN ( '3047745',   --physical therapy Gait distance
										    '3046874',   --ambulation distance
											'3040102774', --post void residual cath
											'10713938',  --pre admission counseling
											'6' ,         --temp
											'14',			--weight
											'1020100004',  -- date of last liquied
											'1217'  ,--time of last liquid
											'51',      --clear liquids - PO
											'5966',    -- % meals consumed)		      
											'5202',     --Last Bowel Movement Date
											'4423',    --GI Signs/Symptoms
											'304340',   --"Stool Occurrence"
											'305020',   --Stool
											'661980',	--Stool output
											'664202',   --flatus
											'304351',   --"Flatus Occurrence
											'4515')    --Diet/feeding tolerance)	  
        AND ifm.MEAS_VALUE IS NOT NULL 
)
SELECT LOG_ID ,
       PAT_ENC_CSN_ID ,
       FSD_ID ,
       LINE ,
       FLO_MEAS_NAME ,
       FLO_MEAS_ID ,
       Flowsheet_DisplayName ,
       MEAS_VALUE ,
	   MEAS_DATE,
       MEAS_NUMERIC ,
       MEAS_COMMENT ,
       RECORDED_TIME ,
       ENTRY_TIME ,
       ENTRY_USER_ID ,
       Entry_Username ,
       TAKEN_USER_ID ,
       Taken_Username ,
       DUPLICATEABLE_YN ,
       ValueType ,
       RowType ,
       IP_LDA_ID ,
       LDAName ,
       PLACEMENT_DTTM ,
       REMOVAL_DTTM ,
       LDA_Description ,
       LDA_Properties ,
       admissioncsnflag ,
       anesthesiacsnflag ,
       PAT_NAME ,
       PAT_MRN_ID ,
	   amb_adlib,
	   amb_bedtfchair,
	   amb_inroom,
	   amb_inhall,
	   amb_25ft,
	   amb_50ft,
	   amb_75ft,
	   amb_100ft,
	   amb_200ft,
       pt_bedtochair ,
       pt_chairtobed ,
       pt_sidesteps ,
       pt_5ft ,
       pt_10ft ,
       pt_15ft ,
       pt_20ft ,
       pt_25ft ,
       pt_50ft ,
       pt_75ft ,
       pt_100ft ,
       pt_150ft ,
       pt_200ft ,
       pt_250ft ,
       pt_300ft ,
       pt_350ft ,
       pt_400ft ,
       pt_x2 ,
       pt_x3 ,
       ambulate_num ,
	   ambulate_den,
	   preadmit_tm,
	   anes_minus2,
	   anes_minus3,
	   presurg,
       preop ,
       intraop ,
       pacu ,
       postop0 ,
	   postop0_6pm,
       postopday1 ,
       postopday2 ,
       postopday3 ,
       afterpostopday4 ,
       Postop_disch ,
       PhaseofCare_id,
	   PhaseofCare_desc,
   	   ProcEnd,
	   DischargeTime

INTO    RADB.dbo.CRD_ERAS_ghgi_FlowDetail
FROM baseq
ORDER BY pat_enc_csn_id,recorded_time;


UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail
SET		admissioncsnflag=0
,		anesthesiacsnflag=0
		,amb_adlib=0
	   ,amb_bedtfchair=0
	   ,amb_inroom=0
	   ,amb_inhall=0
	   ,amb_25ft=0
	   ,amb_50ft=0
	   ,amb_75ft=0
	   ,amb_100ft=0
	   ,amb_200ft=0
     ,	pt_bedtochair=0
	,	pt_chairtobed=0
	,	pt_sidesteps=0
	,	pt_5ft=0	       
	,	pt_10ft=0
	,	pt_15ft=0
	,	pt_20ft=0
	,	pt_25ft=0
	,	pt_50ft=0
	,	pt_75ft=0
	,	pt_100ft=0
	,	pt_150ft=0
	,	pt_200ft=0
	,	pt_250ft=0
	,	pt_300ft=0
	,	pt_350ft=0
	,	pt_400ft=0
	,	pt_x2=0
	,	pt_x3	=0
	,	ambulate_num=0
	,	ambulate_den=0
	,   presurg=0
	,   preop=0
	,   intraop=0
	,   pacu=0
	,  postop0=0	
	,   Postop_disch=0
	,afterpostopday4=0;


WITH baseamb AS (
SELECT v.Value,s.fsd_id,s.line,
pt_bedtochair =  CASE WHEN RTRIM(LTRIM(value)) = 'bed to chair' THEN 1 ELSE 0 END,
pt_chairtobed =  CASE WHEN RTRIM(LTRIM(value)) = 'chair to bed' THEN 1 ELSE 0 END,
pt_sidesteps =  CASE WHEN RTRIM(LTRIM(value)) = 'sidesteps' THEN 1 ELSE 0 END,
pt_5ft =  CASE WHEN RTRIM(LTRIM(value)) = '5 feet' THEN 1 ELSE 0 END,
pt_10ft =  CASE WHEN RTRIM(LTRIM(value)) = '10 feet' THEN 1 ELSE 0 END,
pt_15ft =  CASE WHEN RTRIM(LTRIM(value)) = '15 feet' THEN 1 ELSE 0 END,
pt_20ft =  CASE WHEN RTRIM(LTRIM(value)) = '20 feet' THEN 1 ELSE 0 END,
pt_25ft =  CASE WHEN RTRIM(LTRIM(value)) = '25 feet' THEN 1 ELSE 0 END,
pt_50ft =  CASE WHEN RTRIM(LTRIM(value)) = '50 feet' THEN 1 ELSE 0 END,
pt_75ft =  CASE WHEN RTRIM(LTRIM(value)) = '75 feet' THEN 1 ELSE 0 END,
pt_100ft =  CASE WHEN RTRIM(LTRIM(value)) = '100 feet' THEN 1 ELSE 0 END,
pt_150ft =  CASE WHEN RTRIM(LTRIM(value)) = '150 feet' THEN 1 ELSE 0 END,
pt_200ft =  CASE WHEN RTRIM(LTRIM(value)) = '200 feet' THEN 1 ELSE 0 END,
pt_250ft =  CASE WHEN RTRIM(LTRIM(value)) = '250 feet' THEN 1 ELSE 0 END,
pt_300ft =  CASE WHEN RTRIM(LTRIM(value)) = '300 feet' THEN 1 ELSE 0 END,
pt_350ft =  CASE WHEN RTRIM(LTRIM(value)) = '350 feet' THEN 1 ELSE 0 END,
pt_400ft =  CASE WHEN RTRIM(LTRIM(value)) = '400 feet' THEN 1 ELSE 0 END,
pt_x2 =  CASE WHEN RTRIM(LTRIM(value)) = 'x2' THEN 1 ELSE 0 END,
pt_x3 =  CASE WHEN RTRIM(LTRIM(value)) = 'x3' THEN 1 ELSE 0 END,
amb_adlib =CASE WHEN RTRIM(LTRIM(value)) = 'ambulate ad lib' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_bedtfchair=CASE WHEN RTRIM(LTRIM(value)) = 'ambulate bed to/from chair' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_inroom=CASE WHEN RTRIM(LTRIM(value)) = 'ambulate in room' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_inhall=CASE WHEN RTRIM(LTRIM(value)) = 'ambulate in hall' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_25ft=CASE WHEN RTRIM(LTRIM(value)) = '25 ft' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_50ft=CASE WHEN RTRIM(LTRIM(value)) = '50 ft' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_75ft=CASE WHEN RTRIM(LTRIM(value)) = '75 ft' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_100ft=CASE WHEN RTRIM(LTRIM(value)) = '100 ft' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END,
amb_200ft=CASE WHEN RTRIM(LTRIM(value)) = '200 ft' AND s.flo_meas_id='3046874' THEN 1 ELSE 0 END

FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail s
CROSS APPLY radb.dbo.YNHH_SplitToTable(meas_value,';') AS v
WHERE s.FLO_MEAS_ID IN ('3047745','3046874')
), rolled AS (SELECT 
fsd_id,
line,
amb_adlib=SUM(amb_adlib),
amb_bedtfchair=SUM(amb_bedtfchair),
amb_inroom=SUM(amb_inroom),
amb_inhall=SUM(amb_inhall),
amb_25ft=SUM(amb_25ft),
amb_50ft=SUM(amb_50ft),
amb_75ft=SUM(amb_75ft),
amb_100ft=SUM(amb_100ft),
amb_200ft=SUM(amb_200ft),
pt_bedtochair=SUM(pt_bedtochair),
pt_chairtobed=SUM(pt_chairtobed),
pt_sidesteps=SUM(pt_sidesteps),
pt_5ft=SUM(pt_5ft),
pt_10ft=SUM(pt_10ft),
pt_15ft=SUM(pt_15ft),
pt_20ft=SUM(pt_20ft),
pt_25ft=SUM(pt_25ft),
pt_50ft=SUM(pt_50ft),
pt_75ft=SUM(pt_75ft),
pt_100ft=SUM(pt_100ft),
pt_150ft=SUM(pt_150ft),
pt_200ft=SUM(pt_200ft),
pt_250ft=SUM(pt_250ft),
pt_300ft=SUM(pt_300ft),
pt_350ft=SUM(pt_350ft),
pt_400ft=SUM(pt_400ft),
pt_x2=SUM(pt_x2),
pt_x3=SUM(pt_x3)
 
FROM baseamb
GROUP BY fsd_id,line
)
UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail
SET 
amb_adlib=v.amb_adlib,
amb_bedtfchair=v.amb_bedtfchair,
	   amb_inroom=v.amb_inroom,
	   amb_inhall=v.amb_inhall,
	   amb_25ft=v.amb_25ft,
	   amb_50ft=v.amb_50ft,
	   amb_75ft=v.amb_75ft,
	   amb_100ft=v.amb_100ft,
	   amb_200ft=v.amb_200ft,
pt_chairtobed =   v.pt_chairtobed,
pt_sidesteps =   v.pt_sidesteps,
pt_bedtochair =   v.pt_bedtochair ,
pt_5ft =   v.pt_5ft,
pt_10ft =   v.pt_10ft,
pt_15ft =   v.pt_15ft,
pt_20ft =   v.pt_20ft,
pt_25ft =   v.pt_25ft,
pt_50ft =   v.pt_50ft,
pt_75ft =   v.pt_75ft,
pt_100ft =   v.pt_100ft,
pt_150ft =   v.pt_150ft,
pt_200ft =   v.pt_200ft,
pt_250ft =   v.pt_250ft,
pt_300ft =   v.pt_300ft,
pt_350ft =   v.pt_350ft,
pt_400ft =   v.pt_400ft,
pt_x2 =   v.pt_x2,
pt_x3 =   v.pt_x3
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail a
JOIN rolled v ON a.fsd_id=v.fsd_id AND a.line=v.line;

									

--update all meas_numeric field
UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
SET MEAS_NUMERIC=CAST(MEAS_VALUE AS NUMERIC(13,4))
WHERE ValueType IN ('Numeric Type','Temperature')

--update all meas_numeric field
UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
SET MEAS_DATE=DATEADD(DAY,CAST(MEAS_VALUE AS INT),'12/31/1840')
WHERE ValueType ='Date'


--update all timestamps
	
UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
SET preop=CASE WHEN fs.RECORDED_TIME>=eoc.inpreprocedure AND fs.RECORDED_TIME<=COALESCE(eoc.outofpreprocedure,eoc.anesstart) THEN 1 ELSE 0 END
	,presurg=CASE WHEN fs.RECORDED_TIME>=eoc.HOSP_ADMSN_TIME AND fs.RECORDED_TIME<=eoc.SCHED_START_TIME THEN 1 ELSE 0 END
	, intraop= CASE WHEN fs.RECORDED_TIME>=eoc.inroom AND fs.RECORDED_TIME<=eoc.outofroom THEN 1 ELSE 0 END	
	, preadmit_tm = CASE WHEN fs.RECORDED_TIME>=DATEADD(DAY,-7,eoc.HOSP_ADMSN_TIME) AND fs.RECORDED_TIME < eoc.HOSP_ADMSN_TIME THEN 1 ELSE 0 end
	, anes_minus3 = CASE WHEN fs.RECORDED_TIME>= eoc.SURGERY_DATE AND fs.RECORDED_TIME <= DATEADD(HOUR,-3,eoc.anesstart) THEN 1 ELSE 0 END
    , anes_minus2 = CASE WHEN fs.RECORDED_TIME>=eoc.SURGERY_DATE AND fs.RECORDED_TIME<=DATEADD(HOUR,-3,eoc.inroom)  THEN 1 ELSE 0 end
	, pacu = CASE WHEN fs.RECORDED_TIME>=eoc.inpacu AND fs.RECORDED_TIME<=eoc.outofpacu THEN 1 ELSE 0 END
	, postop0_6pm=CASE WHEN (fs.RECORDED_TIME>=COALESCE(eoc.procedurefinish,eoc.outofroom) AND fs.RECORDED_TIME<eoc.postopday1_begin )
							AND CONVERT(TIME,fs.RECORDED_TIME)<='18:00'
										THEN 1 ELSE 0 END
	, postop0=CASE WHEN fs.RECORDED_TIME>=COALESCE(eoc.procedurefinish,eoc.outofroom) AND fs.RECORDED_TIME<eoc.postopday1_begin THEN 1 ELSE 0 END
	, postopday1=CASE WHEN fs.RECORDED_TIME>=eoc.postopday1_begin AND fs.RECORDED_TIME <eoc.postopday2_begin THEN 1 ELSE 0 END
	, postopday2=CASE WHEN fs.RECORDED_TIME>=eoc.postopday2_begin AND fs.RECORDED_TIME <eoc.postopday3_begin THEN 1 ELSE 0 END
	, postopday3=CASE WHEN fs.RECORDED_TIME>=eoc.postopday3_begin AND fs.RECORDED_TIME <eoc.postopday4_begin THEN 1 ELSE 0 END	
	,afterpostopday4=CASE WHEN fs.RECORDED_TIME>=eoc.postopday4_begin THEN 1 ELSE 0 END	
	,postop_disch=CASE WHEN fs.RECORDED_TIME>=eoc.outofroom AND fs.RECORDED_TIME<=eoc.HOSP_DISCH_TIME THEN 1 ELSE 0 END     	


FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail fs
LEFT JOIN radb.dbo.crd_eras_ghgi_case AS  eoc ON fs.PAT_ENC_CSN_ID=eoc.admissioncsn;

--update phase of care
UPDATE RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
SET PhaseOfCare_id=CASE 
					 WHEN postop0=1 THEN 0
					 WHEN postopday1=1 THEN 1
					 WHEN postopday2=1 THEN 2
					 WHEN postopday3=1 THEN 3
					 WHEN afterpostopday4=1 THEN 4
					 WHEN preop=1 THEN 5
					 WHEN intraop=1 THEN 6
					 WHEN pacu=1 THEN 7
					 WHEN presurg=1 THEN 8
				END,

		PhaseOfCare_desc=CASE WHEN preop=1 THEN 'Preop'
					 WHEN intraop=1 THEN 'Intraop'
					 WHEN pacu=1 THEN 'PACU'
					 WHEN postop0=1 THEN 'POD0'
					 WHEN postopday1=1 THEN 'POD1'
					 WHEN postopday2=1 THEN 'POD2'
					 WHEN postopday3=1 THEN 'POD3'
					 WHEN afterpostopday4=1 THEN 'POD4 or later'
					 WHEN presurg=1 THEN 'PreSurg'
				END;




--Roll out priority 1,2,5,8,9,11,12,14,16 first.

--done
--1- preadmission
--5- NORMAL temp


--update patient weight - first documented in encounter

WITH patweight AS (
select rid=row_number() over(partition by pat_enc_csn_id order by recorded_time)
,cast(meas_value as decimal(13,2)) as weight_oz
,cast(meas_value as decimal(13,2)) * 0.0283495 AS weight_kg
,PAT_ENC_CSN_ID
from RADB.dbo.CRD_ERAS_GHGI_FlowDetail
where flo_meas_id='14'
)UPDATE RADB.dbo.CRD_ERAS_GHGI_EncDim
SET patient_weight_oz=wgt.weight_oz
,patient_weight_kg=wgt.weight_kg
FROM RADB.dbo.CRD_ERAS_GHGI_EncDim AS e
JOIN patweight AS wgt ON wgt.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
WHERE wgt.rid=1;


--populate pacu temp and normal temp on pacu


--1.1 populate last liquid metric 2hours before induction
UPDATE radb.dbo.crd_eras_ghgi_case 
SET clearliquids_3ind=1
FROM radb.dbo.crd_eras_ghgi_case AS c
JOIN (
SELECT PAT_ENC_CSN_ID,COUNT(DISTINCT f.FLO_MEAS_ID) AS colcount
FROM  RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS f
WHERE f.FLO_MEAS_ID IN ('1020100004','1217')
AND f.anes_minus2=1
--AND f.anes_minus3=1
GROUP BY PAT_ENC_CSN_ID
) AS lastliq ON lastliq.PAT_ENC_CSN_ID=c.admissioncsn
WHERE lastliq.colcount=2; --both time and date columns need to be populated


--Metric 3.2 Lumbar epidural

IF object_id('radb.dbo.TMP_epi') IS NOT NULL
	DROP TABLE radb.dbo.TMP_epi;


WITH basesmart AS (	
SELECT  ev.HLV_ID
,		op.PAT_ENC_CSN_ID
,		cc.ABBREVIATION AS ElementName
,		sed.ELEMENT_ID
,		sed.UPDATE_DATE
,		sed.CUR_VAL_UTC_DTTM
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
			       JOIN radb.dbo.crd_eras_ghgi_case AS f
				   ON anescsn=op.PAT_ENC_CSN_ID				   
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
        AND SMRTDTA_ELEM_VALUE='lumbar'
		)SELECT DISTINCT pat_enc_csn_id
		INTO radb.dbo.TMP_epi
		FROM basesmart;
		
UPDATE radb.dbo.CRD_ERAS_GHGI_Case
SET lumbar_epi=CASE WHEN epi.pat_enc_csn_id IS NOT NULL THEN 1 ELSE 0 end
FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
LEFT JOIN radb.dbo.TMP_epi AS epi ON c.anescsn=epi.pat_enc_csn_id
WHERE c.OpenVsLaparoscopic='Open';



--7.1 - normal temp in pacu

UPDATE radb.dbo.crd_eras_ghgi_case 
SET  pacutemp=temp.temp
,NormalTempInPacu=CASE WHEN temp.temp>=96.8 THEN 1 ELSE 0 end
FROM radb.dbo.crd_eras_ghgi_case c
JOIN (
SELECT rid=ROW_NUMBER() OVER(PARTITION BY PAT_ENC_CSN_ID ORDER BY RECORDED_TIME)
,PAT_ENC_CSN_ID ,RECORDED_TIME, MEAS_NUMERIC AS temp
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
WHERE FLO_MEAS_ID='6'					
AND pacu=1
) temp ON temp.PAT_ENC_CSN_ID=c.admissioncsn
WHERE temp.rid=1


--populate time in pacu

UPDATE radb.dbo.crd_eras_ghgi_case 
SET timeinpacu_min=DATEDIFF(MINUTE,inpacu,outofpacu);


--update metric 9.1 clear liquids POD0

UPDATE radb.dbo.crd_eras_ghgi_case
SET clearliquids_pod0=1
FROM radb.dbo.crd_eras_ghgi_case AS c
JOIN (
SELECT PAT_ENC_CSN_ID
FROM  RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
WHERE FLO_MEAS_ID='51'
AND PhaseofCare_desc='POD0'
GROUP BY PAT_ENC_CSN_ID
) AS liqpod0 ON liqpod0.PAT_ENC_CSN_ID=c.admissioncsn;




--update metric 9.2 clear liquids POD1

UPDATE radb.dbo.crd_eras_ghgi_case
SET clearliquids_pod1=1
FROM radb.dbo.crd_eras_ghgi_case AS c
JOIN (
SELECT PAT_ENC_CSN_ID
FROM  RADB.dbo.CRD_ERAS_ghgi_FlowDetail 
WHERE FLO_MEAS_ID='51'
AND PhaseofCare_desc='POD1'
GROUP BY PAT_ENC_CSN_ID
) AS liqpod0 ON liqpod0.PAT_ENC_CSN_ID=c.admissioncsn;



--metric 9.3 solid food POD2
WITH food AS (
SELECT 
		pcteaten=CASE WHEN ISNUMERIC(REPLACE(meas_value,'%',''))=1 THEN CONVERT(INT,REPLACE(meas_value,'%','')) ELSE NULL end
		,*
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS cefd
WHERE FLO_MEAS_ID='5966'
AND postopday2=1
)UPDATE radb.dbo.crd_eras_ghgi_case
SET solidfood_pod2=1
FROM radb.dbo.crd_eras_ghgi_case AS c
JOIN (
SELECT PAT_ENC_CSN_ID 
 FROM food
 WHERE pcteaten>=50
 GROUP BY PAT_ENC_CSN_ID 
 ) AS pod0food ON pod0food.PAT_ENC_CSN_ID=c.admissioncsn;



--ambulation pod0 10.1

WITH ambpod0 AS(		  
SELECT  PAT_ENC_CSN_ID,PhaseofCare_desc,
		totalamb=pt_chairtobed +pt_sidesteps +pt_bedtochair+pt_5ft +pt_10ft 
		  +pt_15ft +pt_20ft +pt_25ft +pt_50ft +pt_75ft +pt_100ft +pt_150ft +pt_200ft +pt_250ft +pt_300ft +pt_350ft +pt_400ft +
		  amb_adlib +      amb_bedtfchair +  amb_inroom + amb_inhall + amb_25ft + amb_50ft + amb_75ft + amb_100ft +amb_200ft 
		  ,f.RECORDED_TIME
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS  f
WHERE f.FLO_MEAS_ID IN ('3047745','3046874')
AND postop0=1
), ambtotal AS (SELECT PAT_ENC_CSN_ID,SUM(totalamb) AS totalamb
FROM ambpod0
GROUP BY PAT_ENC_CSN_ID
HAVING SUM(totalamb)>=1
)UPDATE radb.dbo.crd_eras_ghgi_case
 SET ambulatepod0=1
 FROM radb.dbo.crd_eras_ghgi_case AS c
 JOIN ambtotal AS amb ON c.admissioncsn=amb.PAT_ENC_CSN_ID;

--ambulate pod 1 metric 10.2
WITH ambpod1 AS(		  
SELECT  PAT_ENC_CSN_ID,PhaseofCare_desc,
		totalamb=pt_chairtobed +pt_sidesteps +pt_bedtochair+pt_5ft +pt_10ft 
		  +pt_15ft +pt_20ft +pt_25ft +pt_50ft +pt_75ft +pt_100ft +pt_150ft +pt_200ft +pt_250ft +pt_300ft +pt_350ft +pt_400ft +
		  amb_adlib +      amb_bedtfchair +  amb_inroom + amb_inhall + amb_25ft + amb_50ft + amb_75ft + amb_100ft +amb_200ft 
		  ,f.RECORDED_TIME
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS  f
WHERE f.FLO_MEAS_ID IN ('3047745','3046874')
AND postopday1=1
), ambtotal AS (SELECT PAT_ENC_CSN_ID,SUM(totalamb) AS totalamb
FROM ambpod1
GROUP BY PAT_ENC_CSN_ID
HAVING SUM(totalamb)>=2
)
UPDATE radb.dbo.crd_eras_ghgi_case
 SET ambulate_pod1=1
 FROM radb.dbo.crd_eras_ghgi_case AS c
 JOIN ambtotal AS amb ON c.admissioncsn=amb.PAT_ENC_CSN_ID;

 
--ambulate pod 2 metric 10.3
WITH ambpod2 AS(		  
SELECT  PAT_ENC_CSN_ID,PhaseofCare_desc,
		totalamb=pt_chairtobed +pt_sidesteps +pt_bedtochair+pt_5ft +pt_10ft 
		  +pt_15ft +pt_20ft +pt_25ft +pt_50ft +pt_75ft +pt_100ft +pt_150ft +pt_200ft +pt_250ft +pt_300ft +pt_350ft +pt_400ft +
		  amb_adlib +      amb_bedtfchair +  amb_inroom + amb_inhall + amb_25ft + amb_50ft + amb_75ft + amb_100ft +amb_200ft 
		  ,f.RECORDED_TIME
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS  f
WHERE f.FLO_MEAS_ID IN ('3047745','3046874')
AND postopday2=1
), ambtotal AS (SELECT PAT_ENC_CSN_ID,SUM(totalamb) AS totalamb
FROM ambpod2
GROUP BY PAT_ENC_CSN_ID
HAVING SUM(totalamb)>=2
)UPDATE radb.dbo.crd_eras_ghgi_case
 SET ambulate_pod2=1
 FROM radb.dbo.crd_eras_ghgi_case AS c
 JOIN ambtotal AS amb ON c.admissioncsn=amb.PAT_ENC_CSN_ID;


--update again for those where POD2 is discharge date
WITH ambpod2 AS(		  
SELECT  PAT_ENC_CSN_ID,PhaseofCare_desc,
		totalamb=pt_chairtobed +pt_sidesteps +pt_bedtochair+pt_5ft +pt_10ft 
		  +pt_15ft +pt_20ft +pt_25ft +pt_50ft +pt_75ft +pt_100ft +pt_150ft +pt_200ft +pt_250ft +pt_300ft +pt_350ft +pt_400ft +
		  amb_adlib +      amb_bedtfchair +  amb_inroom + amb_inhall + amb_25ft + amb_50ft + amb_75ft + amb_100ft +amb_200ft 
		  ,f.RECORDED_TIME
FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS  f
LEFT JOIN radb.dbo.crd_eras_ghgi_case AS cec ON f.PAT_ENC_CSN_ID=cec.admissioncsn
WHERE f.FLO_MEAS_ID IN ('3047745','3046874')
AND postopday2=1
AND CONVERT(DATE,cec.HOSP_DISCH_TIME)=CONVERT(DATE,cec.postopday2_begin)
), ambtotal AS (SELECT PAT_ENC_CSN_ID,SUM(totalamb) AS totalamb
FROM ambpod2
GROUP BY PAT_ENC_CSN_ID
HAVING SUM(totalamb)>=1
)UPDATE radb.dbo.crd_eras_ghgi_case
 SET ambulate_pod2=1
 FROM radb.dbo.crd_eras_ghgi_case AS c
 JOIN ambtotal AS amb ON c.admissioncsn=amb.PAT_ENC_CSN_ID;


 --12.1 return of bowel function

 UPDATE radb.dbo.crd_eras_ghgi_case
 SET date_bowelfunction=NULL,hrs_tobowelfunction=NULL;

 --return of bowel function
 WITH basebow AS (
 SELECT PAT_ENC_CSN_ID,CASE WHEN flo_meas_id IN ('304340','305020', '661980','304351') THEN RECORDED_TIME
							WHEN flo_meas_id IN ('5202') THEN MEAS_DATE
							END AS RECORDED_TIME,
							ProcEnd,
							DischargeTime

FROM RADB.dbo.CRD_ERAS_ghgi_FlowDetail AS cefd
WHERE FLO_MEAS_ID IN ('5202',     --Last Bowel Movement Date    - date value
					 '304340',   --"Stool Occurrence" -->0
					'305020',   --Stool -->0
					'661980',	--Stool output   ---> 0				
					'304351')    --- >0
	  AND ((flo_meas_id IN ('304340','305020','661980','304351') AND meas_numeric>0)
	      OR (FLO_MEAS_ID='5202' AND ISDATE(meas_date) =1)		  
		  )
	  
),basebow2 AS (
	SELECT *
	FROM basebow
	WHERE RECORDED_TIME>=ProcEnd AND RECORDED_TIME<=DischargeTime
	)
,bowrolled AS (
SELECT pat_enc_csn_id
,MIN(recorded_time) AS mindt
FROM basebow2
GROUP BY PAT_ENC_CSN_ID
),baseflat AS (
SELECT  v.Value
,       s.FSD_ID
,       s.LINE
,       FLO_MEAS_ID
,       PAT_ENC_CSN_ID
,       RECORDED_TIME
,		GIFlatus=CASE WHEN flo_meas_id='4423' AND value='passing flatus' THEN 1 ELSE 0 END
,		StomaFlatus=CASE WHEN flo_meas_id='664202' AND value='flatus' THEN 1 ELSE 0 END
FROM    RADB.dbo.CRD_ERAS_ghgi_FlowDetail s
        CROSS APPLY RADB.dbo.YNHH_SplitToTable(MEAS_VALUE, ';') AS v
WHERE   FLO_MEAS_ID IN ( '664202', '4423' )
AND (s.RECORDED_TIME>=s.ProcEnd AND s.RECORDED_TIME<=s.DischargeTime)
--AND PAT_ENC_CSN_ID=107451704
--ORDER BY RECORDED_TIME
), flatrolled AS (SELECT pat_enc_csn_id
,MIN(RECORDED_TIME) AS mindt
 FROM baseflat
 WHERE (GIFlatus=1 OR StomaFlatus=1)
GROUP BY pat_enc_csn_id
), unionstation AS (SELECT 'bow' AS src,PAT_ENC_CSN_ID,mindt
 FROM bowrolled
 UNION ALL 
 SELECT 'flat' AS src,pat_enc_csn_id,mindt
 FROM flatrolled
 ),final AS (
 SELECT u.PAT_ENC_CSN_ID,MIN(u.mindt) AS mindt
 FROM unionstation AS u
 GROUP BY PAT_ENC_CSN_ID
 )
 UPDATE radb.dbo.crd_eras_ghgi_case 
SET date_bowelfunction=f.mindt
FROM radb.dbo.crd_eras_ghgi_case AS c
JOIN  final AS f ON f.PAT_ENC_CSN_ID=c.admissioncsn;

--update hours to return of bowel function
 UPDATE radb.dbo.crd_eras_ghgi_case 
SET hrs_tobowelfunction=DATEDIFF(HOUR,COALESCE(procedurefinish,outofroom),date_bowelfunction);




--meds

IF object_id('radb.dbo.CRD_ERAS_GHGI_GivenMeds') IS NOT NULL
	DROP TABLE radb.dbo.CRD_ERAS_GHGI_GivenMeds;

SELECT  mai.MAR_ENC_CSN AS pat_enc_csn_id
		,eo.LOG_ID
		,cm.MEDICATION_ID 		
		,meddim.MedType
		,cm.NAME AS MedicationName
		,cm.FORM AS MedicationForm
		,om.ORDER_MED_ID
		,mai.line
		,rmt.MED_BRAND_NAME
		,empadmin.USER_ID AdminId
		,empadmin.NAME AS AdministeredBy
		,admindep.DEPARTMENT_NAME AS AdministeredDept
	--	,meddim.MedType
		,mai.TAKEN_TIME
		,zcact.NAME AS MarAction
		,maract.MarReportAction
		,mai.mar_action_c
		,mai.SIG AS GivenDose
		,zmu.NAME AS DoseUnit	
		,mai.ROUTE_C					
		,zar.Name AS Route		   
		,CASE WHEN meddim.MedType='Analgesia' THEN
			CASE WHEN mai.route_c IN (15) THEN 'Oral'
				 WHEN mai.route_c IN (155,6,11) THEN 'Parental'
				 ELSE '*Unknown route type'
			END
		END AS Pain_Route
		,mai.DOSE_UNIT_C		
		,preop=0
		,intraop=0
		,pacu=0
		,postop0=0
		,admit_discharge =CAST(NULL AS INT)
		,pacu_disch=CAST(NULL AS INT)  
		,postopday1=CAST(NULL AS INT)
		,postopday1_noon=CAST(NULL AS int)
		,postopday2=CAST(NULL AS INT)
		,postopday3=CAST(NULL AS INT)
		,postop_disch=0
		,admissioncsn_flag=1
		,anescsn_flag=0
		,preproc_inroom=0
		,preproc_outroom=0
		
				

INTO radb.dbo.CRD_ERAS_GHGI_GivenMeds		
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN  (SELECT DISTINCT log_id,admissioncsn	  
	  FROM RADB.dbo.CRD_ERAS_GHGI_Case ) AS eo
ON eo.admissioncsn=mai.MAR_ENC_CSN
LEFT JOIN radb.dbo.CRD_ERAS_GHGI_MarAction AS maract ON maract.RESULT_C=mai.MAR_ACTION_C
LEFT JOIN clarity.dbo.clarity_emp AS empadmin ON empadmin.USER_ID=mai.USER_ID
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.clarity_dep AS admindep ON admindep.DEPARTMENT_ID=mai.MAR_ADMIN_DEPT_ID
LEFT JOIN CLARITY.dbo.RX_MED_THREE AS rmt ON rmt.MEDICATION_ID=cm.MEDICATION_ID
INNER JOIN radb.dbo.CRD_ERAS_GHGI_MedList AS meddim ON meddim.erx=cm.MEDICATION_ID
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
LEFT JOIN clarity.dbo.PATIENT AS p
ON om.PAT_ID=p.PAT_ID
LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C=zar.MED_ROUTE_C


UNION ALL


SELECT  mai.MAR_ENC_CSN AS pat_enc_csn_id        
		,eo.LOG_ID
		,cm.MEDICATION_ID 		
		,meddim.MedType
		,cm.NAME AS MedicationName
		,cm.FORM AS MedicationForm
		,om.ORDER_MED_ID
		,mai.line
		,rmt.MED_BRAND_NAME
		,empadmin.USER_ID AdminId
		,empadmin.NAME AS AdministeredBy
		,admindep.DEPARTMENT_NAME AS AdministeredDept
	--	,meddim.MedType
		,mai.TAKEN_TIME
		,zcact.NAME AS MarAction
		,maract.MarReportAction
		,mai.mar_action_c
		,mai.SIG AS GivenDose
		,zmu.NAME AS DoseUnit						
		,mai.ROUTE_C
		,zar.Name AS Route		
		,CASE WHEN meddim.MedType='Analgesia' THEN
			CASE WHEN mai.route_c IN (15) THEN 'Oral'
				 WHEN mai.route_c IN (155,6,11) THEN 'Parental'
				 ELSE '*Unknown route type'
			END
		END AS Pain_Route
		,mai.DOSE_UNIT_C		
		,preop=0
		,intraop=0
		,pacu=0
		,postop0=0		
		,admit_discharge =CAST(NULL AS INT)
		,pacu_disch=CAST(NULL AS INT)  
		,postopday1=CAST(NULL AS INT)
		,postopday1_noon=CAST(NULL AS int)
		,postopday2=CAST(NULL AS INT)
		,postopday3=CAST(NULL AS INT)
		,postop_disch=0
		,admissioncsn_flag=0
		,anescsn_flag=1			
		,preproc_inroom=0
		,preproc_outroom=0

from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN  (SELECT DISTINCT log_id,anescsn
	  FROM RADB.dbo.CRD_ERAS_GHGI_Case ) AS eo
ON eo.anescsn=mai.MAR_ENC_CSN
LEFT JOIN radb.dbo.CRD_ERAS_GHGI_MarAction AS maract ON maract.RESULT_C=mai.MAR_ACTION_C
LEFT JOIN clarity.dbo.clarity_emp AS empadmin ON empadmin.USER_ID=mai.USER_ID
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.clarity_dep AS admindep ON admindep.DEPARTMENT_ID=mai.MAR_ADMIN_DEPT_ID
LEFT JOIN CLARITY.dbo.RX_MED_THREE AS rmt ON rmt.MEDICATION_ID=cm.MEDICATION_ID
inner JOIN radb.dbo.CRD_ERAS_GHGI_MedList AS meddim ON meddim.erx=cm.MEDICATION_ID
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
LEFT JOIN clarity.dbo.PATIENT AS p
ON om.PAT_ID=p.PAT_ID
LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C=zar.MED_ROUTE_C;


--update given meds phase of care timestamps
UPDATE radb.dbo.CRD_ERAS_GHGI_GivenMeds
SET preop=CASE WHEN TAKEN_TIME>=c.inpreprocedure AND TAKEN_TIME<=c.inroom THEN 1 ELSE 0 END
	, intraop= CASE WHEN TAKEN_TIME>=c.inroom AND TAKEN_TIME<=c.outofroom THEN 1 ELSE 0 END
	, pacu = CASE WHEN TAKEN_TIME>=c.inpacu AND TAKEN_TIME<=c.outofpacu THEN 1 ELSE 0 END
	, postop0=CASE WHEN med.TAKEN_TIME>=c.procedurestart AND med.TAKEN_TIME<c.postopday1_begin THEN 1 ELSE 0 END
	, preproc_inroom=CASE WHEN med.TAKEN_TIME>=c.inpreprocedure AND med.TAKEN_TIME<=c.inroom THEN 1 ELSE 0 END
	, preproc_outroom=CASE WHEN med.TAKEN_TIME>=c.inpreprocedure AND med.TAKEN_TIME<c.outofroom THEN 1 ELSE 0 END
	, postopday1=CASE WHEN med.TAKEN_TIME>=c.postopday1_begin AND med.TAKEN_TIME <c.postopday2_begin THEN 1 ELSE 0 END
	, postopday1_noon=CASE WHEN med.TAKEN_TIME>=c.postopday1_begin AND med.TAKEN_TIME <=DATEADD(HOUR,12,c.postopday1_begin) THEN 1 ELSE 0 END
	, postopday2=CASE WHEN med.TAKEN_TIME>=c.postopday2_begin AND med.TAKEN_TIME <c.postopday3_begin THEN 1 ELSE 0 END
	, postopday3=CASE WHEN med.TAKEN_TIME>=c.postopday3_begin AND med.TAKEN_TIME <c.postopday4_begin THEN 1 ELSE 0 END	
	 ,postop_disch=CASE WHEN TAKEN_TIME>=c.procedurefinish AND TAKEN_TIME<=c.HOSP_DISCH_TIME THEN 1 ELSE 0 END
	 ,pacu_disch=CASE WHEN TAKEN_TIME>=c.inpacu AND TAKEN_TIME<=c.HOSP_DISCH_TIME THEN 1 ELSE 0 end
     ,admit_discharge =CASE WHEN TAKEN_TIME>=c.HOSP_ADMSN_TIME AND TAKEN_TIME<=c.HOSP_DISCH_TIME THEN 1 ELSE 0 end


FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds med
JOIN RADB.dbo.CRD_ERAS_GHGI_Case AS c ON med.pat_enc_csn_id=CASE WHEN med.admissioncsn_flag=1 THEN c.admissioncsn
																 WHEN med.anescsn_flag=1 THEN c.anescsn
																 END;




--update administrations of pain meds

WITH basemar AS (
SELECT rid=ROW_NUMBER() OVER (PARTITION BY ORDER_MED_ID ORDER BY line),*
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE MedType='Analgesia'
AND postop_disch=1
) ,totals AS 
(SELECT LOG_ID,SUM(CASE WHEN Pain_Route='Parental' THEN 1 ELSE 0 END ) AS IVTotal,COUNT(*) AS TotalPain
FROM basemar
WHERE MarReportAction='Given' AND rid=1 --needed because there appear to be dup meds administered
GROUP BY LOG_ID)
UPDATE radb.dbo.CRD_ERAS_GHGI_Case
SET postop_painiv_count=t.IVTotal,
	postop_paintotal_count=t.TotalPain
FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
JOIN totals AS t ON c.LOG_ID=t.LOG_ID;


--antiemtics

WITH basemar AS (
SELECT rid=ROW_NUMBER() OVER (PARTITION BY ORDER_MED_ID ORDER BY line),*
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE MedType='Antiemetic'
AND intraop=1
) 
UPDATE radb.dbo.CRD_ERAS_GHGI_Case
SET mm_antiemetic_intraop=1
FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
JOIN basemar AS t ON c.LOG_ID=t.LOG_ID;


--last pain med

WITH basemar AS (
SELECT rid=ROW_NUMBER() OVER (PARTITION BY LOG_ID ORDER BY TAKEN_TIME desc),*
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE MedType='Analgesia'
AND Pain_Route='Parental'
) 
UPDATE radb.dbo.CRD_ERAS_GHGI_Case
SET date_last_IVpainmed=m.TAKEN_TIME,
	hrs_last_IVpainmed=DATEDIFF(HOUR,c.outofpacu,m.TAKEN_TIME),
	nameof_last_IVpainmed=m.MedicationName,
	last_IVpainmed_adminby=m.AdministeredBy
FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
JOIN basemar AS m ON c.LOG_ID=m.LOG_ID
WHERE m.TAKEN_TIME>=c.outofpacu
AND rid=1;



--iv directed therapy


IF object_id('radb.dbo.TMP_ivintraop_GH') IS NOT NULL
	DROP TABLE radb.dbo.TMP_ivintraop_GH;

WITH baseiv AS (
SELECT  
--b.PAT_NAME
       b.PAT_MRN_ID
	   ,b.admissioncsn
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
,		b.anescsn AS pat_enc_csn_id
,       ifm.RECORDED_TIME
,	CASE WHEN ifm.RECORDED_TIME>=b.inpreprocedure AND ifm.RECORDED_TIME<=b.outofroom
	THEN 1 ELSE 0 END AS intraopflag
,	b.caselength_hrs
,   b.inpreprocedure 
,	b.outofroom

FROM    clarity.dbo.IP_DATA_STORE AS ids
        JOIN RADB.dbo.CRD_ERAS_GHGI_Case AS b
        ON ids.EPT_CSN=b.anescsn
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
		LEFT JOIN Clarity.dbo.IP_FS_ORD_IX_ID ipx 
		ON ipx.INPATIENT_DATA_ID=ifr.INPATIENT_DATA_ID
		AND ifm.OCCURANCE=ipx.GROUP_LINE
		LEFT JOIN clarity.dbo.order_med AS om
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
,e.patient_weight_oz AS weight_oz
,e.patient_weight_kg AS weight_kg
,SUM(f.volume) OVER(PARTITION BY f.pat_enc_csn_id) AS totalvolume
,SUM(CASE WHEN f.intraopflag=1 THEN f.volume ELSE 0 END) OVER(PARTITION BY f.pat_enc_csn_id) AS totalvolume_intraop
FROM baseiv f
JOIN RADB.dbo.CRD_ERAS_GHGI_EncDim AS e
ON f.admissioncsn=e.PAT_ENC_CSN_ID
) , fin AS(
SELECT i.*
,i.weight_kg*caselength_hrs*8 AS threshold
FROM i
),firstcsn AS
(SELECT rid=ROW_NUMBER() OVER(PARTITION BY pat_enc_csn_id ORDER BY pat_enc_csn_id) 
 ,* 
 FROM fin
 )SELECT * 
  INTO radb.dbo.TMP_ivintraop_GH
  FROM firstcsn
  WHERE rid=1;

--update metric on temp table
UPDATE RADB.dbo.CRD_ERAS_GHGI_Case
SET goal_guidelines=1
FROM RADB.dbo.CRD_ERAS_GHGI_Case AS f
JOIN radb.dbo.TMP_ivintraop_GH AS i
ON f.anescsn=i.pat_enc_csn_id
WHERE i.totalvolume_intraop<i.threshold;

--update fact table
UPDATE RADB.dbo.CRD_ERAS_GHGI_Case
SET iv_totalvolume_intraop=i.totalvolume_intraop
,IV_intraop_threshold=i.threshold
FROM RADB.dbo.CRD_ERAS_GHGI_Case f
JOIN radb.dbo.TMP_ivintraop_GH AS i
ON i.pat_enc_csn_id=f.anescsn;


----- ****** goal directed therapy end


--taps block

IF object_id('radb.dbo.TMP_taps_GH') is not null
	drop table radb.dbo.TMP_taps_GH; 

WITH basesmart AS (	
SELECT  rid=ROW_NUMBER() OVER(PARTITION BY op.PAT_ENC_CSN_ID ORDER BY op.ORDER_INST DESC)
,		ev.HLV_ID
,		op.PAT_ENC_CSN_ID
,		op.csntype
,		cc.ABBREVIATION AS ElementName
,		sed.ELEMENT_ID
,		ev.SMRTDTA_ELEM_VALUE
,        op.ORDER_TIME
,		op.ORDER_INST
,		sed.UPDATE_DATE
,		sed.CUR_VAL_UTC_DTTM
,       serauth.PROV_NAME AS AuthProvider
,       serrefer.prov_name AS ReferringProv
,		serperform.PROV_NAME AS PerformingProv
,		p.PAT_MRN_ID
,		p.PAT_NAME
,		op.ORDER_PROC_ID
,		op.PROC_ID
,       op.PROC_CODE
,       op.DESCRIPTION

FROM         Clarity.dbo.Smrtdta_Elem_Data sed               
		   JOIN   (SELECT 'anescsn' AS csntype ,op.*
			       from CLarity.dbo.Order_Proc op	
				   JOIN radb.dbo.CRD_ERAS_GHGI_Case AS c
			       ON op.pat_enc_csn_id=c.anescsn
				   WHERE PROC_id=1127051604
				   UNION all
				   SELECT 'admitcsn',op.*
			       from CLarity.dbo.Order_Proc op	
				   JOIN radb.dbo.CRD_ERAS_GHGI_Case AS c
			       ON op.pat_enc_csn_id=c.admissioncsn
				   WHERE PROC_id=1127051604
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
        AND CUR_VALUE_SOURCE = 'SmartForm 11227804'
       AND (ELEMENT_ID = 'EPIC#12678' AND ev.SMRTDTA_ELEM_VALUE='TAPS')
       	) SELECT * 
		INTO radb.dbo.TMP_taps_GH		
		FROM basesmart
		WHERE rid=1;


UPDATE radb.dbo.CRD_ERAS_GHGI_Case
--SELECT c.anesstart,c.anesfinish,taps.ORDER_INST,CASE WHEN taps.ORDER_INST>=DATEADD(HOUR,-3,c.anesstart) AND taps.ORDER_INST<=DATEADD(HOUR,3,c.anesfinish) THEN 1 ELSE 0 END,
--PerformingProv,
SET tapblock_placed_flag=1,
taps_timeplaced=taps.ORDER_INST,
taps_provider=taps.PerformingProv,
taps_orderid=taps.ORDER_PROC_ID

FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
JOIN radb.dbo.TMP_taps_GH AS taps ON taps.PAT_ENC_CSN_ID=c.anescsn
 AND taps.ORDER_INST>=DATEADD(HOUR,-3,c.anesstart) AND taps.ORDER_INST<=DATEADD(HOUR,3,c.anesfinish);

--foley

IF object_id('tempdb..#foley') is not null
	drop table #foley; 

WITH baseq AS (
SELECT  
'AdmissionCSN' AS EncounterType
,f.LOG_ID
,f.PAT_NAME
,f.PAT_MRN_ID
,f.anescsn
,f.admissioncsn
,f.surgicalcsn
,f.HOSP_ADMSN_TIME
,f.HOSP_DISCH_TIME
,f.surgery_date
,iln.FLO_MEAS_ID
,       iln.PLACEMENT_INSTANT
,       iln.REMOVAL_INSTANT
,		CASE WHEN iln.FLO_MEAS_ID ='8151' THEN  1 ELSE 0 END AS suprapubic
,		CASE WHEN (PLACEMENT_INSTANT >HOSP_ADMSN_TIME AND PLACEMENT_INSTANT <=outofroom )
				  AND (REMOVAL_INSTANT>=f.procedurefinish OR REMOVAL_INSTANT IS NULL )
				  AND iln.FLO_MEAS_ID='8148' THEN 1 ELSE 0 end	AS denflag
				  
,		CASE WHEN (REMOVAL_INSTANT>=f.procedurefinish AND REMOVAL_INSTANT < f.postopday2_begin )
					AND iln.FLO_MEAS_ID='8148' THEN 1 ELSE 0 end	AS numflag

,		CASE WHEN iln.PLACEMENT_INSTANT<f.HOSP_ADMSN_TIME THEN '*Before Admission'
			 WHEN iln.PLACEMENT_INSTANT>=f.HOSP_ADMSN_TIME AND iln.PLACEMENT_INSTANT<f.inroom THEN '*Before In room'
			 WHEN iln.PLACEMENT_INSTANT>=f.inroom AND iln.PLACEMENT_INSTANT<=f.outofroom THEN '*In room'
			 WHEN iln.PLACEMENT_INSTANT>f.outofroom THEN '*After Outofroom' END AS PlacementWindow

,		CASE WHEN iln.REMOVAL_INSTANT<f.HOSP_ADMSN_TIME THEN '*Before Admission'
			 WHEN iln.REMOVAL_INSTANT>=f.HOSP_ADMSN_TIME AND iln.REMOVAL_INSTANT<f.inroom THEN '*Before In room'
			 WHEN iln.REMOVAL_INSTANT>=f.inroom AND iln.REMOVAL_INSTANT<=f.outofroom THEN '*In room'
			 WHEN iln.REMOVAL_INSTANT>f.outofroom THEN '*After Outofroom' END AS RemovalWindow
,f.inroom
,f.outofroom
,f.procedurestart
,f.procedurefinish
,f.postopday1_begin
,f.postopday2_begin
,f.postopday3_begin
,f.postopday4_begin
,       iln.PAT_ENC_CSN_ID
,       iln.IP_LDA_ID
,       iln.DESCRIPTION
,       iln.PROPERTIES_DISPLAY
,		iln.FSD_ID
,		ifgd.DUPLICATEABLE_YN
,		ifgd.FLO_MEAS_NAME
,		ifgd.DISP_NAME
,		rowtype=zrt.name
FROM    (SELECT rid=ROW_NUMBER() OVER(PARTITION BY admissioncsn ORDER BY admissioncsn)
		,*
		 FROM RADB.dbo.CRD_ERAS_GHGI_Case 		
		) AS f
		JOIN clarity.dbo.IP_DATA_STORE AS ids
		ON ids.EPT_CSN=f.admissioncsn
		JOIN clarity.dbo.IP_LDA_INPS_USED AS iliu
		ON ids.INPATIENT_DATA_ID=iliu.INP_ID
		JOIN clarity.dbo.IP_LDA_NOADDSINGLE AS iln 
				ON iln.IP_LDA_ID=iliu.IP_LDA_ID				        
		LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd
		ON iln.FLO_MEAS_ID=ifgd.FLO_MEAS_ID				
		LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt
		ON ifgd.ROW_TYP_C=zrt.ROW_TYP_C       
WHERE   iln.FLO_MEAS_ID IN ( '8148', '8151' )
AND f.rid=1
UNION ALL
SELECT  
'AnesCSN' AS EncounterType
,f.LOG_ID
,f.PAT_NAME
,f.PAT_MRN_ID
,f.anescsn
,f.admissioncsn
,f.surgicalcsn
,f.HOSP_ADMSN_TIME
,f.HOSP_DISCH_TIME
,f.surgery_date
,iln.FLO_MEAS_ID
,       iln.PLACEMENT_INSTANT
,       iln.REMOVAL_INSTANT
,		CASE WHEN iln.FLO_MEAS_ID ='8151' THEN  1 ELSE 0 END AS suprapubic
,		CASE WHEN (PLACEMENT_INSTANT >HOSP_ADMSN_TIME AND PLACEMENT_INSTANT <=outofroom )
				  AND (REMOVAL_INSTANT>=f.procedurefinish OR REMOVAL_INSTANT IS NULL )
				  AND iln.FLO_MEAS_ID='8148' THEN 1 ELSE 0 end	AS denflag
				  
,		CASE WHEN (REMOVAL_INSTANT>=f.procedurefinish AND REMOVAL_INSTANT < f.postopday2_begin )
					AND iln.FLO_MEAS_ID='8148' THEN 1 ELSE 0 end	AS numflag

,		CASE WHEN iln.PLACEMENT_INSTANT<f.HOSP_ADMSN_TIME THEN '*Before Admission'
			 WHEN iln.PLACEMENT_INSTANT>=f.HOSP_ADMSN_TIME AND iln.PLACEMENT_INSTANT<f.inroom THEN '*Before In room'
			 WHEN iln.PLACEMENT_INSTANT>=f.inroom AND iln.PLACEMENT_INSTANT<=f.outofroom THEN '*In room'
			 WHEN iln.PLACEMENT_INSTANT>f.outofroom THEN '*After Outofroom' END AS PlacementWindow

,		CASE WHEN iln.REMOVAL_INSTANT<f.HOSP_ADMSN_TIME THEN '*Before Admission'
			 WHEN iln.REMOVAL_INSTANT>=f.HOSP_ADMSN_TIME AND iln.REMOVAL_INSTANT<f.inroom THEN '*Before In room'
			 WHEN iln.REMOVAL_INSTANT>=f.inroom AND iln.REMOVAL_INSTANT<=f.outofroom THEN '*In room'
			 WHEN iln.REMOVAL_INSTANT>f.outofroom THEN '*After Outofroom' END AS RemovalWindow
,f.inroom
,f.outofroom
,f.procedurestart
,f.procedurefinish
,f.postopday1_begin
,f.postopday2_begin
,f.postopday3_begin
,f.postopday4_begin
,       iln.PAT_ENC_CSN_ID
,       iln.IP_LDA_ID
,       iln.DESCRIPTION
,       iln.PROPERTIES_DISPLAY
,		iln.FSD_ID
,		ifgd.DUPLICATEABLE_YN
,		ifgd.FLO_MEAS_NAME
,		ifgd.DISP_NAME
,		rowtype=zrt.name
FROM    (SELECT rid=ROW_NUMBER() OVER(PARTITION BY admissioncsn ORDER BY admissioncsn)
		,*
		 FROM RADB.dbo.CRD_ERAS_GHGI_Case 		
		) AS f
		JOIN clarity.dbo.IP_DATA_STORE AS ids
		ON ids.EPT_CSN=f.anescsn
		JOIN clarity.dbo.IP_LDA_INPS_USED AS iliu
		ON ids.INPATIENT_DATA_ID=iliu.INP_ID
		JOIN clarity.dbo.IP_LDA_NOADDSINGLE AS iln 
				ON iln.IP_LDA_ID=iliu.IP_LDA_ID				        
		LEFT JOIN clarity.dbo.IP_FLO_GP_DATA AS ifgd
		ON iln.FLO_MEAS_ID=ifgd.FLO_MEAS_ID				
		LEFT JOIN clarity.dbo.ZC_ROW_TYP AS zrt
		ON ifgd.ROW_TYP_C=zrt.ROW_TYP_C       
WHERE   iln.FLO_MEAS_ID IN ( '8148', '8151' )
AND f.rid=1
)SELECT * 
INTO #foley
FROM baseq;


WITH foleyupdate AS (
SELECT log_id,SUM(denflag) AS sumden,SUM(numflag) AS sumnum,SUM(suprapubic) AS suprapubic
FROM #foley AS f
GROUP BY LOG_ID
)UPDATE radb.dbo.CRD_ERAS_GHGI_Case
 SET foleypod1_Num=CASE WHEN f.sumnum=f.sumden THEN 1 ELSE 0 END
 ,foleypod_Den=CASE WHEN f.sumden>0 THEN 1 ELSE 0 END
 ,foleysuprapubic_flag=CASE WHEN f.suprapubic>0 THEN 1 ELSE 0 end
 FROM radb.dbo.CRD_ERAS_GHGI_Case AS c
 JOIN foleyupdate AS f ON c.LOG_ID=f.LOG_ID;


EXEC radb.dbo.CRD_ERAS_GHGI_Create_DateDim;





