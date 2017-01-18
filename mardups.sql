--one row at a time
--WHERE mai.ORDER_MED_ID=179989084
--AND mai.line=1


SELECT *
from clarity.dbo.MAR_ADMIN_INFO AS mai

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
WHERE mai.ORDER_MED_ID=179989084 AND mai.line=1








191077597
1


SELECT *
INTO


SELECT rid=ROW_NUMBER() OVER (PARTITION BY ORDER_MED_ID ORDER BY line),*
--SELECT ORDER_MED_ID,line
--SELECT *
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE ORDER_MED_ID=191077597 AND line=1 --AND pat_enc_csn_id=112483580
GROUP BY ORDER_MED_ID,line
HAVING COUNT(*)>1


WITH basemar AS (
SELECT rid=ROW_NUMBER() OVER (PARTITION BY ORDER_MED_ID ORDER BY line),*
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
) 
SELECT LOG_ID,SUM(CASE WHEN Pain_Route='Parental' THEN 1 ELSE 0 END ) AS IVTotal,COUNT(*) AS TotalPain
FROM basemar
WHERE MarReportAction='Given'
GROUP BY LOG_ID
UNION all
SELECT LOG_ID,SUM(CASE WHEN Pain_Route='Parental' THEN 1 ELSE 0 END ) AS IVTotal,COUNT(*) AS TotalPain
FROM basemar
WHERE MarReportAction='Given' AND rid=1
GROUP BY LOG_ID
ORDER BY log_id




SELECT * 
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE ORDER_MED_ID=179989084

SELECT *
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
WHERE LOG_ID='375938'
SELECT MarReportAction,Pain_Route,COUNT(*)
FROM radb.dbo.CRD_ERAS_GHGI_GivenMeds
GROUP BY MarReportAction,Pain_Route

TRUNCATE table
 radb.dbo.CRD_ERAS_GHGI_MarAction
FROM radb.dbo.CRD_ERAS_GHGI_MarAction


insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (1,'Given','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (6,'New Bag','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (7,'Restarted','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (9,'Rate Change','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (12,'Bolus','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (13,'Push','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (102,'Given by Other','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (105,'New Syringe/Bag','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (110,'Anesthesia Bar Code','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (112,'Anesthesia Volume Adjustment','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (113,'Given During Downtime','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (114,'Started During Downtime','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (115,'Patch Applied','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (117,'Bolus from Bag','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (119,'Given During Downtime.','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (121,'Anesthesia Bolus','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (122,'Bolus from Syringe','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (123,'Bolus from Pump','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (124,'Bolus from Bottle','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (126,'LIP Administered','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (127,'PHARMACYONLY ORAL','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (129,'Dose Change','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (130,'Self Administered','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (134,'INV PO given','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (142,'Given with meal tray in Room','Given')
insert into radb.dbo.CRD_ERAS_GHGI_MarAction VALUES (147,'Bolus from Vial','Given')



UPDATE radb.dbo.CRD_ERAS_GHGI_MedList
SET medtype='Analgesia'




179989084

SELECT *
from clarity.dbo.MAR_ADMIN_INFO AS mai


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
LEFT JOIN clarity.dbo.ZC_ADMIN_ROUTE AS zar ON mai.ROUTE_C=zar.MED_ROUTE_C
WHERE mai.ORDER_MED_ID=179989084 AND mai.line=1 AND mai.MAR_ENC_CSN=112483580