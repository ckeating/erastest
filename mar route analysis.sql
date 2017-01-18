SELECT r 
FROM radb.dbo.CRD_ERAS_GHGI_Case AS cegc

SELECT * 
FROM dbo.CRD_ERAS_YNHGI_MarAction AS ceyma


SELECT *
FROM clarity.dbo.ZC_ADMIN_ROUTE
ORDER BY name

SELECT Route,COUNT(*)
FROM RADB.dbo.CRD_ERAS_GHGI_GivenMeds
GROUP BY Route
ORDER BY 1


IF object_id('radb.dbo.CRD_ERAS_GHGI_GivenMeds') IS NOT NULL
	DROP TABLE RADB.dbo.CRD_ERAS_GHGI_GivenMeds;

SELECT  mai.MAR_ENC_CSN AS pat_enc_csn_id
		,eo.LOG_ID
		,cm.MEDICATION_ID 
		,meddim.MetricNumber
		,meddim.MetricDescription
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
		--,maract.MarReportAction
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
		
				

INTO RADB.dbo.CRD_ERAS_GHGI_GivenMeds		
from clarity.dbo.MAR_ADMIN_INFO AS mai
JOIN  radb.dbo.CRD_ERAS_GHGI_Case  AS eo
ON eo.admissioncsn=mai.MAR_ENC_CSN
--JOIN radb.dbo.CRD_ERAS_YNHGI_MarAction AS maract ON maract.RESULT_C=mai.MAR_ACTION_C
LEFT JOIN clarity.dbo.clarity_emp AS empadmin ON empadmin.USER_ID=mai.USER_ID
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.clarity_dep AS admindep ON admindep.DEPARTMENT_ID=mai.MAR_ADMIN_DEPT_ID
LEFT JOIN CLARITY.dbo.RX_MED_THREE AS rmt ON rmt.MEDICATION_ID=cm.MEDICATION_ID
INNER JOIN radb.dbo.CRD_ERAS_YNHGI_MedList AS meddim ON meddim.erx=cm.MEDICATION_ID
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
LEFT JOIN clarity.dbo.PATIENT AS p
ON om.PAT_ID=p.PAT_ID
LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C=zar.MED_ROUTE_C


UNION ALL


SELECT  mai.MAR_ENC_CSN AS pat_enc_csn_id
		,eo.LOG_ID
		,cm.MEDICATION_ID 
		,meddim.MetricNumber
		,meddim.MetricDescription
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
--		,maract.MarReportAction
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
JOIN  radb.dbo.CRD_ERAS_GHGI_Case   AS eo
ON eo.anescsn=mai.MAR_ENC_CSN
--JOIN radb.dbo.CRD_ERAS_YNHGI_MarAction AS maract ON maract.RESULT_C=mai.MAR_ACTION_C
LEFT JOIN clarity.dbo.clarity_emp AS empadmin ON empadmin.USER_ID=mai.USER_ID
LEFT join clarity.dbo.ORDER_MED AS om
ON mai.ORDER_MED_ID=om.ORDER_MED_ID
LEFT JOIN clarity.dbo.ZC_MED_UNIT AS zmu
ON zmu.DISP_QTYUNIT_C=mai.DOSE_UNIT_C
LEFT JOIN clarity.dbo.clarity_medication cm
ON om.medication_id=cm.medication_id
LEFT JOIN clarity.dbo.clarity_dep AS admindep ON admindep.DEPARTMENT_ID=mai.MAR_ADMIN_DEPT_ID
LEFT JOIN CLARITY.dbo.RX_MED_THREE AS rmt ON rmt.MEDICATION_ID=cm.MEDICATION_ID
inner JOIN radb.dbo.CRD_ERAS_YNHGI_MedList AS meddim ON meddim.erx=cm.MEDICATION_ID
LEFT JOIN clarity.dbo.zc_mar_rslt AS zcact
ON zcact.result_c=mai.mar_action_c
LEFT JOIN clarity.dbo.PATIENT AS p
ON om.PAT_ID=p.PAT_ID
LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C=zar.MED_ROUTE_C;

