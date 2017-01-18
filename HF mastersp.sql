ALTER PROCEDURE [dbo].[usp_CRD_28599_HeartFailure_Master] AS


-- ================================================================================================
-- Author:		Craig Keating
-- Create date: 10/13/2016
-- Description:	CRD 28599 HeartFailure Master procedure
-- ================================================================================================
 

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

--pull population



IF OBJECT_ID('radb.dbo.CRD_HF') IS NOT NULL
	DROP TABLE radb.dbo.CRD_HF;

SELECT					
  p.pat_NAME,
  p.PAT_MRN_ID,
  p.pat_id,
  p.BIRTH_DATE AS DateofBirth,
  PatientAge=COALESCE(DATEDIFF(yy,p.BIRTH_DATE,peh.HOSP_DISCH_TIME),DATEDIFF(yy,p.BIRTH_DATE,GETDATE())),
  zeg.NAME AS HispanicEthnicity,
  zpr.name AS PatientRace,
  zs.name AS Gender,
  peh.PAT_ENC_CSN_ID,
  peh.HSP_ACCOUNT_ID AS HAR,
  lhd.Department_ID,
  lhd.Department_Name AS DischargeDept,
  lhd.Specialty AS DischargeDepartmentSpecialty,
  dischser.PROV_NAME AS DischargeProvider,
  lhd.System_Organization_Name,
  lhd.Campus,  
  peh.HOSP_ADMSN_TIME AS AdmissionTime,
  CONVERT(DATE,peh.HOSP_DISCH_TIME) AS DischargeDate,    
  peh.HOSP_DISCH_TIME AS DischargeTime,    
  peh.DISCH_DISP_C,
  --zdd.name AS DischargeDisposition,
  zps.NAME AS DischargeDisposition,
  peh.ADT_PAT_CLASS_C AS Enc_Pat_class_C,
 zpc.NAME AS Enc_Pat_Class,
  hsp.ACCT_FIN_CLASS_C,
  zfc.NAME AS FinancialClass,
  LOSDays=DATEDIFF(dd,peh.HOSP_ADMSN_TIME,peh.HOSP_DISCH_TIME)    ,
  LOSHours=DATEDIFF(hh,peh.HOSP_ADMSN_TIME,peh.HOSP_DISCH_TIME)    ,  
  icd10.code AS PrimaryCodedICD10Code,
  edg.DX_NAME AS PrimaryCodedDiagnosis	,
  FirstWeight_value=CAST(NULL AS NUMERIC(13,4)),
  FirstWeight_date=CAST(NULL AS DATETIME),
  LastWeight_value =CAST(NULL AS NUMERIC(13,4)),
  LastWeight_date =CAST(NULL AS DATETIME),
  FirstBNP_orderid=cast (NULL AS NUMERIC(18,0)),
  FirstBNP_result=cast (NULL AS NUMERIC(13,4)),
  FirstBNP_collectiontime=cast (NULL AS DATETIME),
  LastBNP_orderid=cast (NULL AS NUMERIC(18,0)),
  LastBNP_result=cast (NULL AS NUMERIC(13,4)),
  LastBNP_collectiontime=cast (NULL AS DATETIME),  
  FirstRothman_score=CAST(NULL AS NUMERIC(13,4)),
  FirstRothman_date =CAST(NULL AS DATETIME),  
  LastRothman_score=CAST(NULL AS NUMERIC(13,4)),
  LastRothman_date = CAST(NULL AS DATETIME),
  RothmanIndexChange=CAST(NULL AS INT),
  OnPathway=CAST(NULL AS INT),
  FirstOS_ordernumber=CAST(NULL AS NUMERIC(18,0)),
  FirstOS_ordername = CAST(NULL AS VARCHAR(510)),
  FirstOS_orderdate= CAST(NULL AS DATETIME),
  Weight_at_pathwaystart=CAST(NULL AS NUMERIC(13,4)),
  Weight_72_postpathway=CAST(NULL AS NUMERIC(13,4)),
  UrineLoss_72hrs_liters=CAST(NULL AS NUMERIC(13,4)),
  UrineLoss_72hrs_ml=CAST(NULL AS NUMERIC(13,4)),
  WeightLoss_72hrs=CAST(NULL AS NUMERIC(13,4)),
  Pathway_plus_72_dttm=cast (NULL AS DATETIME),
  fluidloss1_flag = CAST(NULL AS TINYINT),
  fluidloss2_flag = CAST(NULL AS TINYINT)
	
INTO radb.dbo.CRD_HF

FROM    
     clarity.dbo.PAT_ENC_HSP AS peh 
   LEFT JOIN  clarity.dbo.HSP_ACCOUNT  hsp ON peh.PAT_ENC_CSN_ID=hsp.PRIM_ENC_CSN_ID
   LEFT JOIN  clarity.dbo.HSP_ACCT_DX_LIST AS codedx ON codedx.HSP_ACCOUNT_ID=hsp.HSP_ACCOUNT_ID   
				AND codedx.LINE=1   

   LEFT JOIN clarity.dbo.CLARITY_EDG AS edg ON codedx.DX_ID=edg.DX_ID
   
   LEFT JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS icd10 ON edg.DX_ID=icd10.DX_ID 
												AND icd10.LINE=1	
  
   LEFT JOIN Clarity.dbo.EDG_CURRENT_ICD9 AS icd9  ON edg.DX_ID=icd9.DX_ID 
												AND icd9.LINE=1	

    
   LEFT JOIN clarity.dbo.ZC_MC_PAT_STATUS AS zps ON zps.PAT_STATUS_C=hsp.PATIENT_STATUS_C
   LEFT JOIN clarity.dbo.ZC_DISCH_DISP AS zdd  ON zdd.DISCH_DISP_C=peh.DISCH_DEST_C
   LEFT OUTER JOIN CLARITY.dbo.PATIENT AS p ON (p.PAT_ID=peh.PAT_ID)   
   LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS  AS zpc ON (ZPC.ADT_PAT_CLASS_C=peh.ADT_PAT_CLASS_C )

   LEFT JOIN clarity.dbo.ZC_SEX AS zs ON zs.RCPT_MEM_SEX_C=p.SEX_C
   LEFT JOIN clarity.dbo.CLARITY_DEP AS cd ON peh.DEPARTMENT_ID=cd.DEPARTMENT_ID

   LEFT JOIN radb.dbo.LocHierarchy_Department AS lhd ON lhd.Department_ID=peh.DEPARTMENT_ID AND lhd.Is_Current_YN='Y'

   LEFT JOIN clarity.dbo.ZC_ETHNIC_GROUP AS zeg ON zeg.ETHNIC_GROUP_C=p.ETHNIC_GROUP_C   
   LEFT JOIN clarity.dbo.PATIENT_RACE AS pr ON p.PAT_ID=pr.PAT_ID AND pr.LINE=1
   LEFT JOIN clarity.dbo.ZC_PATIENT_RACE AS zpr ON zpr.PATIENT_RACE_C=pr.PATIENT_RACE_C   
   LEFT JOIN clarity.dbo.ZC_FIN_CLASS AS zfc ON zfc.FIN_CLASS_C=hsp.ACCT_FIN_CLASS_C
   LEFT OUTER JOIN Clarity.dbo.HSP_ATND_PROV  HSP_ATND_PROV_last 
		ON (peh.PAT_ENC_CSN_ID = HSP_ATND_PROV_last.PAT_ENC_CSN_ID  
			AND peh.HOSP_DISCH_TIME 
			BETWEEN HSP_ATND_PROV_last.ATTEND_FROM_DATE AND HSP_ATND_PROV_last.ATTEND_TO_DATE)
	LEFT JOIN CLarity.dbo.clarity_ser AS dischser ON dischser.PROV_ID=HSP_ATND_PROV_last.PROV_ID

WHERE peh.HOSP_DISCH_TIME>'1/1/2015'

AND (icd10.CODE IN ('I11.0','I13.0','I13.2','I50.1','I50.20','I50.21','I50.22',
'I50.23','I50.30','I50.31','I5032','I50.33','I50.40','I50.41','I50.42','I50.43','I50.9')
OR icd9.code IN ('402.01','402.11','402.91','404.01','404.03','404.11','404.13','404.91','404.93','428','428.1','428.2','428.21','428.22','428.23',
'428.3','428.31','428.32','428.33','428.4','428.41','428.42','428.43','428.9'))
AND peh.DEPARTMENT_ID IN ('101010015','102010026');


--build metrics


WITH orderset AS (

SELECT 
rid=ROW_NUMBER() OVER (PARTITION BY om.PAT_ENC_CSN_ID ORDER BY order_dttm)
,hf.PAT_ENC_CSN_ID
,om.ORDER_DTTM
,om.ORDER_ID
,om.DISPLAY_NAME AS 'Order_Display_Name'
,om.ORDER_TYPE_C

FROM   RADB.dbo.CRD_HF AS hf
LEFT JOIN  clarity.dbo.ORDER_METRICS AS om ON om.PAT_ENC_CSN_ID=hf.PAT_ENC_CSN_ID
WHERE om.PRL_ORDERSET_ID=3040002528
) UPDATE radb.dbo.CRD_HF
SET FirstOS_ordernumber=o.ORDER_ID
,FirstOS_ordername=o.Order_Display_Name
,FirstOS_orderdate=o.ORDER_DTTM
FROM radb.dbo.CRD_HF AS hf
JOIN orderset AS o ON o.PAT_ENC_CSN_ID=hf.PAT_ENC_CSN_ID
WHERE o.rid=1;
									
--set orderpathway flag
UPDATE radb.dbo.CRD_HF 
SET OnPathway=CASE WHEN FirstOS_ordernumber IS NOT NULL THEN 1 ELSE 0 END;



IF OBJECT_ID('radb.dbo.tmp22_hf_bnp') IS NOT null
	DROP TABLE radb.dbo.tmp22_hf_bnp;

SELECT     
				rid=ROW_NUMBER() OVER(PARTITION BY OP_PRNT.PAT_ENC_CSN_ID ORDER BY OP2_CHLD.SPECIMN_TAKEN_TIME )
				,OP_CHLD.PAT_ENC_CSN_ID
                --, PAT.PAT_MRN_ID
                --, DOB = PAT.BIRTH_DATE
                --, SEX = CASE PAT.SEX_C WHEN '1' THEN 'Female' WHEN '2' THEN 'Male' ELSE 'Unknown' END
                --, ORDERING_DEPARTMENT = DEP.DEPARTMENT_NAME
		          --, HOSPITAL = CASE WHEN G38.DEP_RPT_GRP_38_C = '101' THEN 'YORK STREET CAMPUS' ELSE G38.TITLE END
				  ,CD34.PAT_MRN_ID
				  ,CD34.pat_NAME
				  ,CD34.AdmissionTime
				  ,CD34.DischargeTime
                , CEAP.PROC_CODE
                , PROC_NAME = CEAP.PROC_NAME
                , ORDER_NUMBER = OP_CHLD.ORDER_PROC_ID
                , ORDER_DATE = OP_CHLD.ORDER_TIME
                , COLLECTION_DATE = OP2_CHLD.SPECIMN_TAKEN_TIME
                , OR_CHLD.LINE
                , COMPONENT = CMP.NAME
                , RESULT = ISNULL(OR_CHLD.ORD_VALUE,'')
				, RESULT_NUM=OR_CHLD.ORD_NUM_VALUE
			INTO radb.dbo.tmp22_hf_bnp                   
           FROM Clarity.dbo.ORDER_PROC AS OP_PRNT WITH(NOLOCK)
				
                INNER JOIN Clarity.dbo.ORDER_INSTANTIATED AS OI WITH(NOLOCK)
                     ON OP_PRNT.ORDER_PROC_ID = OI.ORDER_ID

                INNER JOIN Clarity.dbo.ORDER_PROC AS OP_CHLD WITH(NOLOCK)
                     ON OI.INSTNTD_ORDER_ID = OP_CHLD.ORDER_PROC_ID

                INNER JOIN Clarity.dbo.ORDER_PROC_2 AS OP2_CHLD WITH(NOLOCK) 
                     ON OP2_CHLD.ORDER_PROC_ID = OP_CHLD.ORDER_PROC_ID

                INNER JOIN radb.dbo.CRD_HF AS CD34
                     ON OP_CHLD.PAT_ENC_CSN_ID = CD34.PAT_ENC_CSN_ID

                LEFT JOIN Clarity.dbo.ZC_ORDER_TYPE AS ZOT
                     ON OP_CHLD.ORDER_TYPE_C = ZOT.INTERNAL_ID

                LEFT JOIN Clarity.dbo.ORDER_RESULTS OR_CHLD WITH(NOLOCK) 
                     ON OP_CHLD.ORDER_PROC_ID = OR_CHLD.ORDER_PROC_ID

                LEFT JOIN Clarity.dbo.ZC_RESULT_FLAG AS ZRF
                     ON OR_CHLD.RESULT_FLAG_C = ZRF.INTERNAL_ID

                LEFT JOIN Clarity.dbo.CLARITY_COMPONENT AS CMP WITH(NOLOCK) 
                     ON OR_CHLD.COMPONENT_ID = CMP.COMPONENT_ID

                --LEFT JOIN Clarity.dbo.ORDER_RES_COMMENT AS ORC WITH(NOLOCK)
                --   ON OR_CHLD.ORDER_PROC_ID = ORC.ORDER_ID AND OR_CHLD.LINE = ORC.LINE

                INNER JOIN Clarity.dbo.PAT_ENC_HSP AS PEH WITH(NOLOCK) 
                     ON PEH.PAT_ENC_CSN_ID = OP_PRNT.PAT_ENC_CSN_ID

                INNER JOIN Clarity.dbo.PATIENT AS PAT WITH(NOLOCK)
                     ON PEH.PAT_ID = PAT.PAT_ID

                LEFT JOIN Clarity.dbo.HSP_ACCOUNT AS HAR WITH(NOLOCK)
                     ON PEH.HSP_ACCOUNT_ID = HAR.HSP_ACCOUNT_ID

                LEFT JOIN Clarity.dbo.CLARITY_DEP AS DEP WITH(NOLOCK)
                     ON OP2_CHLD.PAT_LOC_ID = DEP.DEPARTMENT_ID

                LEFT JOIN Clarity.dbo.ZC_DEP_RPT_GRP_38 AS G38 WITH(NOLOCK)
                     ON DEP.RPT_GRP_TRTYEIGHT_C = G38.DEP_RPT_GRP_38_C

                LEFT JOIN Clarity.dbo.ZC_CONF_STAT AS CNF_STAT WITH(NOLOCK)
                     ON PEH.ADMIT_CONF_STAT_C = CNF_STAT.ADMIT_CONF_STAT_C

                LEFT JOIN Clarity.dbo.ZC_ORDER_STATUS AS CHLD_OS WITH(NOLOCK)
                     ON OP_CHLD.ORDER_STATUS_C = CHLD_OS.ORDER_STATUS_C

                LEFT JOIN Clarity.dbo.CLARITY_EAP AS CEAP WITH(NOLOCK)
                     ON OP_CHLD.PROC_ID = CEAP.PROC_ID

                LEFT JOIN Clarity.dbo.ZC_LAB_STATUS AS CHLD_LAB_STAT WITH(NOLOCK)
                     ON OP_CHLD.LAB_STATUS_C = CHLD_LAB_STAT.LAB_STATUS_C

                LEFT JOIN Clarity.dbo.ZC_REASON_FOR_CANC AS CHLD_Cancel WITH(NOLOCK)
                     ON CHLD_Cancel.REASON_FOR_CANC_C=OP_CHLD.REASON_FOR_CANC_C
     
           WHERE  (ISNULL(CHLD_OS.NAME, 'Active') <> 'Canceled'     
                     OR  OP_CHLD.ORDER_STATUS_C=4 AND OP_CHLD.REASON_FOR_CANC_C=14)
                     AND OP_CHLD.IS_PENDING_ORD_YN = 'N'
                     AND OP_CHLD.LAB_STATUS_C IS NOT NULL
                     AND OP_CHLD.SERV_AREA_ID = '10'
                     AND OP_CHLD.PROC_ID IN (89011) --(CBC: LAB293 = 1696) (CD34: LAB2752 = 87927)
                     AND OP2_CHLD.SPECIMN_TAKEN_TIME IS NOT NULL
                     --AND G38.DEP_RPT_GRP_38_C IN ('101','102') --(103,104)
                     --AND HAR.HSP_ACCOUNT_NAME NOT LIKE 'TEST%TEST%'
                     AND (NOT HAR.ACCT_BILLSTS_HA_C IN (40,99) AND HAR.COMBINE_ACCT_ID IS NULL)
					 --AND OP_PRNT.PAT_ENC_CSN_ID IN (SELECT PAT_ENC_CSN_ID FROM radb.dbo.crd_hf)
					 AND CD34.OnPathway=1
					ORDER BY  OP_PRNT.PAT_ENC_CSN_ID;


--update BNP's

UPDATE radb.dbo.CRD_HF

SET FirstBNP_orderid=bnp.firstorderid,
	FirstBNP_result=REPLACE(bnp.firstresult,',',''),
	FirstBNP_collectiontime=bnp.firstcollectiondate
	
FROM radb.dbo.CRD_HF AS HF
JOIN (SELECT bnp.PAT_ENC_CSN_ID,
	bnp.COLLECTION_DATE AS firstcollectiondate,
	bnp.ORDER_NUMBER AS firstorderid,
	bnp.RESULT AS firstresult

FROM radb.dbo.tmp22_hf_bnp AS bnp
join 
(SELECT pat_enc_csn_id,MIN(rid)  AS minrid
FROM radb.dbo.tmp22_hf_bnp AS bnp
GROUP BY pat_enc_csn_id
) AS minrid ON minrid.PAT_ENC_CSN_ID = bnp.PAT_ENC_CSN_ID
			AND minrid.minrid=bnp.rid
		) bnp ON bnp.PAT_ENC_CSN_ID=hf.PAT_ENC_CSN_ID


		
UPDATE radb.dbo.CRD_HF

SET LastBNP_orderid=bnp.lastorderid,
	LastBNP_result=REPLACE(bnp.lastresult,',',''),
	LastBNP_collectiontime=bnp.lastcollectiondate
	
FROM radb.dbo.CRD_HF AS HF
JOIN (SELECT bnp.PAT_ENC_CSN_ID,
	bnp.COLLECTION_DATE AS lastcollectiondate,
	bnp.ORDER_NUMBER AS lastorderid,
	bnp.RESULT AS lastresult

FROM radb.dbo.tmp22_hf_bnp AS bnp
join 
(SELECT pat_enc_csn_id,MAX(rid)  AS maxrid
FROM radb.dbo.tmp22_hf_bnp AS bnp
GROUP BY pat_enc_csn_id
HAVING MAX(rid)<>1
) AS maxrid ON maxrid.PAT_ENC_CSN_ID = bnp.PAT_ENC_CSN_ID
			AND maxrid.maxrid=bnp.rid
		) bnp ON bnp.PAT_ENC_CSN_ID=hf.PAT_ENC_CSN_ID



					 
--get weights

IF object_id('radb.dbo.tmp_hfweight') is not null
	drop table radb.dbo.tmp_hfweight; 


--WITH baseweight AS (
SELECT  
hf.pat_NAME
,hf.PAT_MRN_ID
,hf.AdmissionTime
,hf.DischargeTime
,       ifgd.DUPLICATEABLE_YN
,		ifgd.VAL_TYPE_C
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		ifm.line
,		ids.EPT_CSN
,       ifm.RECORDED_TIME
,		ifm.flo_meas_id
,       ifm.MEAS_VALUE
,		weight_kg=CAST(ifm.MEAS_VALUE AS NUMERIC (13,4))/35.27392
,       ifm.MEAS_COMMENT
,		rid=ROW_NUMBER() OVER(PARTITION BY ept_csn order BY ifm.recorded_time)
--,		ifgd.*
INTO    radb.dbo.tmp_hfweight
FROM    clarity.dbo.IP_DATA_STORE AS ids                
		JOIN radb.dbo.CRD_HF AS hf ON ids.EPT_CSN=hf.PAT_ENC_CSN_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
WHERE   ifm.FLO_MEAS_ID IN ( '14' ) --1020100004 -date 1217 time
        AND ifm.MEAS_VALUE IS NOT NULL
        ORDER BY ids.EPT_CSN,ifm.RECORDED_TIME;


UPDATE radb.dbo.CRD_HF
SET FirstWeight_value=hfweight.firstweight
	,FirstWeight_date=hfweight.firsttime
	,LastWeight_value=hfweight.lastweight
	,LastWeight_date=hfweight.lasttime
FROM radb.dbo.CRD_HF AS hf
 LEFT JOIN (
SELECT aggwgt.ept_csn,firstweight=firstwgt.weight_kg,
		lastweight=	CASE WHEN lastwgt.rid=1 THEN NULL ELSE lastwgt.weight_kg END,
			firsttime=firstwgt.RECORDED_TIME,
			lasttime=CASE WHEN lastwgt.rid=1 THEN NULL ELSE lastwgt.RECORDED_TIME END 
From
(SELECT ept_csn,MIN(rid) AS minrid,MAX(rid) AS maxrid
 FROM radb.dbo.tmp_hfweight
 GROUP BY ept_csn
 ) AS aggwgt
 LEFT JOIN radb.dbo.tmp_hfweight AS firstwgt ON firstwgt.ept_csn=aggwgt.ept_csn
							   AND firstwgt.rid=aggwgt.minrid
LEFT JOIN radb.dbo.tmp_hfweight AS lastwgt ON lastwgt.ept_csn=aggwgt.ept_csn
							   AND lastwgt.rid=aggwgt.maxrid
) hfweight ON hfweight.ept_csn=hf.pat_enc_csn_id;



IF object_id('radb.dbo.tmp_hfroth') is not null
	drop table radb.dbo.tmp_hfroth; 


SELECT  
hf.pat_NAME
,hf.PAT_MRN_ID
,hf.AdmissionTime
,hf.DischargeTime
,       ifgd.DUPLICATEABLE_YN
,		ifgd.VAL_TYPE_C
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		ifm.line
,		ids.EPT_CSN
,       ifm.RECORDED_TIME
,		ifm.flo_meas_id
,       ifm.MEAS_VALUE
,		weight_kg=CAST(ifm.MEAS_VALUE AS NUMERIC (13,4))/35.27392
,       ifm.MEAS_COMMENT
,		rid=ROW_NUMBER() OVER(PARTITION BY ept_csn order BY ifm.recorded_time)
--,		ifgd.*
INTO    radb.dbo.tmp_hfroth 
FROM    clarity.dbo.IP_DATA_STORE AS ids                
		JOIN radb.dbo.CRD_HF AS hf ON ids.EPT_CSN=hf.PAT_ENC_CSN_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
WHERE   ifm.FLO_MEAS_ID IN ( '13328' ) 
        AND ifm.MEAS_VALUE IS NOT NULL
        ORDER BY ids.EPT_CSN,ifm.RECORDED_TIME


UPDATE radb.dbo.CRD_HF
SET FirstRothman_score=hfroth.firstroth
	,FirstRothman_date=hfroth.firsttime
	,LastRothman_score=hfroth.lastroth
	,LastRothman_date=hfroth.lasttime
FROM radb.dbo.CRD_HF AS hf
 LEFT JOIN (
SELECT aggroth.ept_csn,firstroth=firstroth.meas_value,
		lastroth=	CASE WHEN lastroth.rid=1 THEN NULL ELSE lastroth.meas_value END,
			firsttime=firstroth.RECORDED_TIME,
			lasttime=CASE WHEN lastroth.rid=1 THEN NULL ELSE lastroth.RECORDED_TIME END 
From
(SELECT ept_csn,MIN(rid) AS minrid,MAX(rid) AS maxrid
 FROM radb.dbo.tmp_hfroth
GROUP BY ept_csn
 ) AS aggroth
 LEFT JOIN radb.dbo.tmp_hfroth AS firstroth ON firstroth.ept_csn=aggroth.ept_csn
							   AND firstroth.rid=aggroth.minrid
LEFT JOIN radb.dbo.tmp_hfroth AS lastroth ON lastroth.ept_csn=aggroth.ept_csn
							   AND lastroth.rid=aggroth.maxrid
) hfroth ON hfroth.ept_csn=hf.pat_enc_csn_id;

--update rothman index flag

UPDATE radb.dbo.CRD_HF
SET RothmanIndexChange=CASE WHEN LastRothman_score<FirstRothman_score THEN 1 ELSE 0 END;




UPDATE radb.dbo.CRD_HF 
SET Pathway_plus_72_dttm=DATEADD(HOUR,72,FirstOS_orderdate),
	UrineLoss_72hrs_liters=NULL,
	UrineLoss_72hrs_ml=null;



WITH urineloss AS (
SELECT  
hf.pat_NAME
,hf.PAT_MRN_ID
,hf.AdmissionTime
,hf.DischargeTime
,hf.FirstOS_orderdate
,       ifgd.DUPLICATEABLE_YN
,		ifgd.VAL_TYPE_C
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		ifm.line
,		ids.EPT_CSN
,       ifm.RECORDED_TIME
,		ifm.flo_meas_id
,       ifm.MEAS_VALUE
,		weight_kg=CAST(ifm.MEAS_VALUE AS NUMERIC (13,4))/35.27392
,       ifm.MEAS_COMMENT
,		rid=ROW_NUMBER() OVER(PARTITION BY ept_csn order BY ifm.recorded_time)
,		urineloss_72=CASE WHEN ifm.RECORDED_TIME<=Pathway_plus_72_dttm THEN ifm.MEAS_VALUE ELSE 0 END
--,		ifgd.*
--INTO    radb.dbo.tmp_hfroth 
FROM    clarity.dbo.IP_DATA_STORE AS ids                
		JOIN radb.dbo.CRD_HF AS hf ON ids.EPT_CSN=hf.PAT_ENC_CSN_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
WHERE   ifm.FLO_MEAS_ID IN ( '61' ) 
        AND ifm.MEAS_VALUE IS NOT NULL
		AND hf.OnPathway=1
) UPDATE radb.dbo.CRD_HF
SET UrineLoss_72hrs_liters= loss.totalurine 
FROM radb.dbo.CRD_HF AS h
JOIN     (SELECT ept_csn,SUM(urineloss_72 ) AS totalurine
		FROM urineloss
		GROUP BY ept_csn
		) AS loss ON loss.EPT_CSN=h.PAT_ENC_CSN_ID;

UPDATE radb.dbo.CRD_HF
SET UrineLoss_72hrs_ml= UrineLoss_72hrs_liters/1000;




IF OBJECT_ID('radb.dbo.tmp22_weightloss') IS NOT NULL
	DROP TABLE radb.dbo.tmp22_weightloss;

WITH weight1 AS (
SELECT  
hf.PAT_ENC_CSN_ID
,hf.pat_NAME
,hf.PAT_MRN_ID
,hf.AdmissionTime
,hf.DischargeTime
,       ifgd.DUPLICATEABLE_YN
,		ifgd.VAL_TYPE_C
,       zvt.name AS ValueType
,       zrt.name AS RowType
,       ifgd.FLO_MEAS_NAME
,       ifgd.DISP_NAME
,       ifm.FSD_ID
,		ifm.line
,		ids.EPT_CSN
,hf.FirstOS_orderdate
,       ifm.RECORDED_TIME
,		ifm.flo_meas_id
,       ifm.MEAS_VALUE
--,		weight_kg=CAST(ifm.MEAS_VALUE AS NUMERIC (13,4))/35.27392
,       ifm.MEAS_COMMENT
,		rid=ROW_NUMBER() OVER(PARTITION BY ept_csn order BY ifm.recorded_time)
,		weightloss_72=CASE WHEN ifm.RECORDED_TIME>=hf.FirstOS_orderdate AND ifm.RECORDED_TIME<=Pathway_plus_72_dttm THEN convert(numeric(13,4),ifm.MEAS_VALUE) ELSE 0 END
,		hf.Pathway_plus_72_dttm
,		in72window=CASE WHEN ifm.RECORDED_TIME>=hf.FirstOS_orderdate AND ifm.RECORDED_TIME<=Pathway_plus_72_dttm THEN 1 ELSE 0 END
--,		ifgd.*
--INTO    radb.dbo.tmp_hfroth 
FROM    clarity.dbo.IP_DATA_STORE AS ids                
		JOIN radb.dbo.CRD_HF AS hf ON ids.EPT_CSN=hf.PAT_ENC_CSN_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_REC AS ifr ON ids.INPATIENT_DATA_ID = ifr.INPATIENT_DATA_ID
        LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS AS ifm ON ifr.FSD_ID = ifm.FSD_ID
        LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA AS ifgd ON ifm.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
        LEFT JOIN Clarity.dbo.ZC_VAL_TYPE AS zvt ON zvt.VAL_TYPE_C = ifgd.VAL_TYPE_C
        LEFT JOIN Clarity.dbo.ZC_ROW_TYP AS zrt ON zrt.ROW_TYP_C = ifgd.ROW_TYP_C
WHERE   ifm.FLO_MEAS_ID IN ( '14' ) 
        AND ifm.MEAS_VALUE IS NOT NULL
		AND hf.OnPathway=1
) ,weight2 AS (
		SELECT * 
		,winid=ROW_NUMBER() OVER(PARTITION BY ept_csn ,in72window ORDER BY ept_csn )		
		FROM weight1
		),fin AS (SELECT *
		,rootweight=CASE WHEN in72window=1 THEN LAG(MEAS_VALUE,winid,'NA') OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY RECORDED_TIME ) ELSE NULL END        
				 FROM weight2
				 )  SELECT f.*,
				 weightlossoz=CONVERT(NUMERIC(13,4),rootweight-weightloss_72)/35.27348542
				 INTO radb.dbo.tmp22_weightloss
				  FROM fin AS f
				  --WHERE PAT_ENC_CSN_ID=130830180
				  JOIN (SELECT pat_enc_csn_id,MAX(winid) AS maxwin
				        FROM fin 
						WHERE in72window=1
						GROUP BY pat_enc_csn_id) AS maxid ON maxid.PAT_ENC_CSN_ID = f.PAT_ENC_CSN_ID
														  AND maxid.maxwin=f.winid
				  WHERE f.in72window=1
ORDER BY PAT_ENC_CSN_ID,RECORDED_TIME;

UPDATE radb.dbo.CRD_HF
SET Weight_at_pathwaystart=wt.rootweight
,	Weight_72_postpathway=wt.weightloss_72
,	WeightLoss_72hrs=wt.weightlossoz
FROM radb.dbo.CRD_HF AS hf
JOIN  radb.dbo.tmp22_weightloss AS wt ON hf.PAT_ENC_CSN_ID=wt.PAT_ENC_CSN_ID;


UPDATE radb.dbo.CRD_HF
SET fluidloss1_flag=CASE WHEN UrineLoss_72hrs_ml>9 OR WeightLoss_72hrs>=3 THEN 1 ELSE 0 END,
	fluidloss2_flag=CASE WHEN UrineLoss_72hrs_ml>9 AND WeightLoss_72hrs>=3 THEN 1 ELSE 0 END;



EXEC radb.dbo.CRD_HF_BuildDateDim;



SELECT MetricName ,
       MetricCalculation ,
       Type ,
       TrendOrd ,
       MeasureGroup ,
       full_date ,
       week_begin_date ,
       Num ,
       Den ,
       pat_NAME ,
       PAT_MRN_ID ,
       pat_id ,
       DateofBirth ,
       PatientAge ,
       HispanicEthnicity ,
       PatientRace ,
       Gender ,
       PAT_ENC_CSN_ID ,
       HAR ,
       Department_ID ,
       DischargeDept ,
       DischargeDepartmentSpecialty ,
       DischargeProvider ,
       System_Organization_Name ,
       Campus ,
       AdmissionTime ,
       DischargeDate ,
       DischargeTime ,
       DischargeDateKey ,
       DischargeDisposition ,
       Enc_Pat_Class ,
       FinancialClass ,
       LOSDays ,
       LOSHours ,
       PrimaryCodedICD10Code ,
       PrimaryCodedDiagnosis ,
       FirstWeight_value ,
       FirstWeight_date ,
       LastWeight_value ,
       LastWeight_date ,
       FirstBNP_orderid ,
       FirstBNP_result ,
       FirstBNP_collectiontime ,
       LastBNP_orderid ,
       LastBNP_result ,
       LastBNP_collectiontime ,
       FirstRothman_score ,
       FirstRothman_date ,
       LastRothman_score ,
       LastRothman_date ,
       RothmanIndexChange_DEN ,
       [Decreased Rothman score?] ,
       OnPathway_NUM ,
       [On Pathway?] ,
       FirstOS_ordernumber ,
       FirstOS_ordername ,
       FirstOS_orderdate ,
       Weight_at_pathwaystart ,
       Weight_72_postpathway ,
       UrineLoss_72hrs_liters ,
       UrineLoss_72hrs_ml ,
       WeightLoss_72hrs ,
       Pathway_plus_72_dttm ,
       fluidloss1_flag ,
       fluidloss2_flag ,
       HospitalWide_30DayReadmission_DEN ,
       HospitalWide_30DayReadmission_NUM ,
	   RptGrouper AS OnPathwayGrouper
FROM radb.dbo.vw_CRD_HF_Report;



