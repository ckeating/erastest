SELECT *
FROM dbo.vw_PatEnc AS vpe 
WHERE --PAT_ENC_CSN_ID=137391662
 PAT_MRN_ID='MR38101'
AND CONTACT_DATE>'8/25/2016'


SELECT OP.PAT_ENC_CSN_ID
       , PEH.ADT_ARRIVAL_TIME AS ED_ARRIVAL_TIME
       , OP.PAT_ID
       , OP.ORDER_PROC_ID
       , OP.PROC_ID
       , OP.PROC_CODE
       , OP.DESCRIPTION AS PROC_DESCRIPTION
       , OP.DISPLAY_NAME AS PROC_NAME
       , OP.ORDERING_DATE
       , OP.ORDER_INST
       , OP.ORDER_TIME
       , OP2.SPECIMN_TAKEN_TIME
       , OP.PAT_ENC_DATE_REAL
       , VIS.BEGIN_EXAM_DTTM AS IMG_BEGIN_EXAM_DTTM
       , VIS.END_EXAM_DTTM AS IMG_END_EXAM_DTTM
       , VIS.FINALIZING_DTTM AS IMG_FINALIZING_DTTM
       , OP2.PAT_LOC_ID AS ORDERING_DEPARTMENT_ID
       , DEP.DEPARTMENT_NAME AS ORDERING_DEPARTMENT
       , DEP.SPECIALTY 
        , DEP.DEP_ED_TYPE_C
       , OP.IS_PENDING_ORD_YN
       , OP.ORDER_STATUS_C
       , ZOS.NAME AS ORDER_STATUS
       , OP.REASON_FOR_CANC_C
       , ZCR.NAME AS REASON_FOR_CANC
       , CASE 
                     WHEN (
                                  OP.ORDER_STATUS_C <> 4 /* Order not cancelled  */ 
                                  OR (OP.ORDER_STATUS_C = 4 AND ISNULL(OP.REASON_FOR_CANC_C, -1) = 14 /* Patient Discharge */)
                           ) THEN
                           0
                     ELSE
                           1
          END AS ORDER_CANCELED_FLAG
       , PEH.ED_DISPOSITION_C
       , EAP.PROC_CAT_ID
       , PCAT.PROC_CAT_NAME 
        , ORES.LINE AS ORDER_RESULTS_LINE_NUM
       , ORES.COMPONENT_ID
       , CASE WHEN ORES.COMPONENT_ID = 1577876 /* WBC */ THEN 'Y' ELSE 'N' END AS WBC_COMPONENT_YN
       , CASE WHEN ORES.COMPONENT_ID IN (1511105, 1526296) THEN 'Y' ELSE 'N' END AS CREATININE_COMPONENT_YN
       , ORES.RESULT_TIME
       , COMP.ABBREVIATION AS COMPONENT_ABBR
       , COMP.EXTERNAL_NAME AS COMPONENT_NAME
       , COMP.LOINC_CODE AS COMPONENT_LOINC_CODE
       , ORES.ORD_VALUE AS ORDER_RESULTS_VALUE_ORIG
       , TRY_PARSE(ORES.ORD_VALUE AS NUMERIC(18, 2)) AS ORDER_RESULTS_VALUE
       , ORES.REF_UNIT_UOM_ID AS ORDER_RESULTS_UOM_ID
       , UOM.UNIT_NAME AS ORDER_RESULTS_UOM
       , ZLAB_STAT.NAME AS LAB_STATUS
FROM Clarity.dbo.ORDER_PROC AS OP WITH(NOLOCK)
LEFT JOIN Clarity.dbo.CLARITY_EAP AS EAP WITH(NOLOCK)
       ON OP.PROC_ID = EAP.PROC_ID
LEFT JOIN Clarity.dbo.EDP_PROC_CAT_INFO AS PCAT WITH(NOLOCK)
       ON EAP.PROC_CAT_ID = PCAT.PROC_CAT_ID
LEFT JOIN Clarity.dbo.PAT_ENC_HSP AS PEH WITH(NOLOCK)
       ON OP.PAT_ENC_CSN_ID = PEH.PAT_ENC_CSN_ID
LEFT JOIN Clarity.dbo.ZC_REASON_FOR_CANC AS ZCR WITH(NOLOCK)
       ON ZCR.REASON_FOR_CANC_C = OP.REASON_FOR_CANC_C
LEFT JOIN Clarity.dbo.ZC_ORDER_STATUS AS ZOS WITH(NOLOCK)
       ON ZOS.ORDER_STATUS_C = OP.ORDER_STATUS_C
LEFT JOIN Clarity.dbo.ORDER_PROC_2 AS OP2 WITH(NOLOCK)
       ON OP.ORDER_PROC_ID = OP2.ORDER_PROC_ID
LEFT JOIN Clarity.dbo.ORDER_RESULTS AS ORES WITH(NOLOCK)
       ON OP.ORDER_PROC_ID = ORES.ORDER_PROC_ID
LEFT JOIN Clarity.dbo.ZC_LAB_STATUS AS ZLAB_STAT WITH(NOLOCK)
       ON OP.LAB_STATUS_C = ZLAB_STAT.LAB_STATUS_C
LEFT JOIN Clarity.dbo.CLARITY_COMPONENT AS COMP WITH(NOLOCK)
       ON COMP.COMPONENT_ID = ORES.COMPONENT_ID
LEFT JOIN Clarity.dbo.UNIT_OF_MEASURE AS UOM WITH(NOLOCK)
       ON UOM.UNIT_ID = ORES.REF_UNIT_UOM_ID
LEFT JOIN Clarity.dbo.V_IMG_STUDY AS VIS WITH(NOLOCK)
       ON OP.ORDER_PROC_ID = VIS.ORDER_ID
INNER JOIN Clarity.dbo.CLARITY_DEP AS DEP WITH(NOLOCK) 
       ON OP2.PAT_LOC_ID = DEP.DEPARTMENT_ID
          --AND OP2.PAT_LOC_ID IN () ----Insert departments IDs here
WHERE (
                     OP.ORDER_STATUS_C <> 4 /* Order not cancelled  */ 
                     OR (OP.ORDER_STATUS_C = 4 AND ISNULL(OP.REASON_FOR_CANC_C, -1) = 14 /* Patient Discharge */) --Discontinued due to patient discharge
         )
         AND ISNULL(OP.IS_PENDING_ORD_YN, 'N') = 'N'
         
         /* Child order Start */
         AND OP.FUTURE_OR_STAND IS NULL 
         AND OP.INSTANTIATED_TIME IS NOT NULL
         /* Child order End */
AND op.PAT_ENC_CSN_ID=137391662


SELECT * FROM dbo.CRD_ERAS_GHGI_Case AS cegc
WHERE admissioncsn=133839427



IF object_id('tempdb..##taps') is not null
	drop table ##taps; 

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
--,op.*
,       op.PROC_CODE
,       op.DESCRIPTION
--,ev.*
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
		INTO ##taps		
		FROM basesmart
		WHERE rid=1;




		SELECT DISTINCT pat_enc_csn_id
		INTO radb.dbo.TMP_epi
		FROM basesmart;

		SELECT * 
		FROM ##taps

		ORDER BY pat_enc_csn_id
		

		SELECT * FROM radb.dbo.vw_PatEnc AS vpe
		WHERE PAT_ENC_CSN_ID=121862878