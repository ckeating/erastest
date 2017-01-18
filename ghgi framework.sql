SELECT * FROM dbo.CRD_ERAS_MetricDim AS cemd

SELECT * FROM CRD_ERAS_GHGI_MetricDim WHERE MetricType='process' AND MetricName LIKE'%solid%'



SELECT * FROM CRD_ERAS_GHGI_MetricDim WHERE MetricName LIKE'%any%'


SELECT DATEdiff(DAY,c.procedurefinish, e.Discharge_DTTM)
FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS c
LEFT join radb.dbo.vw_CRD_ERAS_GHGI_EncDim e ON c.AdmissionCSN=e.CSN


SELECT  *
FROM dbo.vw_CRD_ERAS_GHGI_Case AS vcegc
WHERE Log_ID IN (SELECT log_id FROM dbo.vw_CRD_ERAS_GHGI_Case GROUP BY Log_ID HAVING COUNT(*)>1)
ORDER BY AdmissionCSN

ALTER TABLE CRD_ERAS_GHGI_MetricDim
ADD Active int

UPDATE dbo.CRD_ERAS_GHGI_MetricDim
SET metric

ROLLBACK
BEGIN tran
DELETE CRD_ERAS_GHGI_MetricDim 
WHERE id IN (21,22,26,28,30,

 SELECT * 
 FROM dbo.CRD_ERAS_GHGI_MetricDim 
 WHERE active=1
 ORDER BY MetricType,MetricName
 
 UPDATE  dbo.CRD_ERAS_GHGI_MetricDim 
 SET Active=1 WHERE id IN (9)

 WHERE MetricName LIKE '%liquid%'

CREATE TABLE  dbo.CRD_ERAS_GHGI_MetricDim
(ID INT IDENTITY(1,1),
 CategoryID INT,
 MetricID NUMERIC(10,2),
 MetricCategory varchar(100),
 MetricName VARCHAR(100),
 MetricCalculation VARCHAR(100),
 MetricType VARCHAR(100),
 TrendOrd INT
 )

 INSERT dbo.CRD_ERAS_GHGI_MetricDim
         ( MetricName
         ,MetricCalculation
         ,MetricType
		 ,TrendOrd
         )
 SELECT 
       MetricName 
 ,      MetricCalculation
 ,      MetricType
 ,      TrendOrd
 
 FROM dbo.CRD_ERAS_MetricDim AS cemd


SELECT * FROM dbo.CRD_ERAS_MetricFact AS cemf

 SELECT * 
 FROM dbo.CRD_ERAS_GHGI_MetricDim 
-- WHERE active=1
 ORDER BY MetricType,MetricName

 SELECT * FROM dbo.vw_CRD_ERAS_Report_Detail AS vcerd



CREATE VIEW dbo.CRD_ERAS_GHGI_MetricFact
AS

SELECT --median los
		CAST('1' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter AS ERASRptGrouper
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
	   ,ah.Discharge_DateKey
	   ,1 AS Num
	   ,1 AS Den
	FROM
		radb.dbo.vw_CRD_ERAS_GHGI_EncDim AS  ah

UNION ALL

SELECT --average los
		CAST('1' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter
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
	   ,Discharge_DateKey AS DateKey
	   ,proclos AS Num
	   ,1 'Den'
	FROM	(
		SELECT  e.csn, e.Discharge_DateKey,DATEDIFF(DAY,c.procedurefinish, e.Discharge_DTTM) AS proclos,e.ERASEncounter
		FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS c
		LEFT join radb.dbo.vw_CRD_ERAS_GHGI_EncDim e ON c.AdmissionCSN=e.CSN
		) AS c

		
UNION ALL

SELECT --avg los proc to disch
		CAST('61' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter AS ERASRptGrouper
	   ,Discharge_DateKey AS DateKey
	   ,proclos AS Num
	   ,1 'Den'
	FROM	(
		SELECT  e.csn, e.Discharge_DateKey,DATEDIFF(DAY,c.procedurefinish, e.Discharge_DTTM) AS proclos,e.ERASEncounter
		FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS c
		LEFT join radb.dbo.vw_CRD_ERAS_GHGI_EncDim e ON c.AdmissionCSN=e.CSN
		) AS c


UNION ALL



SELECT --readmission rate
		CAST('3' AS INT) AS 'MetricKey'
	   ,ISNULL(csn,NULL) 'PAT_ENC_CSN_ID'
	   ,NULL AS Log_ID
	   ,ERASEncounter
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
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_3ind AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah

UNION ALL

SELECT --3.2 % open cases with lumbar epidural
		CAST('56' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.lumbar_epi AS Num
	   ,1 Den
	FROM radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah
		 WHERE ah.ProcedureType='Open'


UNION ALL

SELECT --7.1 % normal PACU temperature
		CAST('51' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.NormalTempInPacu AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_Case AS ah

UNION ALL 

SELECT --% 9.1 PO liquids POD 0
		CAST('57' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_pod0 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah


UNION ALL

SELECT --% 9.2 PO liquids POD 1
		CAST('58' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.clearliquids_pod1 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah

UNION ALL

SELECT --% 9.3 Solid foods POD 2
		CAST('60' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.solidfood_pod2  AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah

UNION ALL


SELECT --10.1 % ambulate day 0
		CAST('52' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.ambulatepod0,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.ambulatepod0 AS Num
	   ,1 AS Den
	FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah

UNION ALL

SELECT --10.2 ambulate pod 1
		CAST('53' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod1 as Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah


UNION ALL

SELECT --10.3 ambulate pod 2
		CAST('54' AS INT) AS 'MetricKey'
	   ,ISNULL(ah.admissioncsn,NULL) 'PAT_ENC_CSN_ID'
	   , ah.LOG_ID AS Log_ID
	   ,ErasCase
	   ,ah.SurgeryDateKey
	   ,ah.ambulate_pod2 as Num
	   ,1 AS Den
		FROM
		 radb.dbo.vw_CRD_ERAS_GHGI_Case AS ah


UNION ALL


SELECT --12.1 % return of bowel function
		CAST('59' AS INT) AS 'MetricKey'
	   ,ISNULL(AdmissionCSN,NULL) 'PAT_ENC_CSN_ID'
	   , Log_ID
	   ,ErasCase
	   ,SurgeryDateKey
	   ,CASE WHEN hrs_tobowelfunction IS NOT NULL THEN 1 ELSE 0 end
	   ,1 AS Den
	FROM 
		radb.dbo.vw_CRD_ERAS_GHGI_Case 



-- ********************** end of metric fact



		SELECT * FROM 
vw_CRD_ERAS_EncDim



CREATE VIEW dbo.vw_CRD_ERAS_GHGI_EncDim 
AS
SELECT  
		f.PAT_ENC_CSN_ID AS CSN ,
        f.HSP_ACCOUNT_ID AS HAR ,
        f.pat_name AS PatientName ,
        f.pat_mrn_id AS MRN ,
        f.LOSDays ,
        f.LOSHours ,
        f.HOSP_ADMSN_TIME AS Admission_DTTM ,
        CONVERT(DATE, f.HOSP_ADMSN_TIME) AS Admission_DT ,
        f.HOSP_DISCH_TIME AS Discharge_DTTM ,
        CONVERT(DATE, f.HOSP_DISCH_TIME) AS Discharge_DT ,
        f.Discharge_DateKey ,
		ERASEncounter=   CASE WHEN erasflag.erascount>0 THEN 'Eras Case' ELSE 'Non-ERAS Case' END ,
        f.Enc_DischargeDisposition ,
        f.PatientStatus ,
        f.BaseClass ,
        f.Enc_Pat_Class ,
        f.[Admission Type] ,
        f.HospitalWide_30DayReadmission_NUM ,
        f.HospitalWide_30DayReadmission_DEN ,
        f.TotalDirectCost ,
        f.NumberofProcs ,		
        qvi_Infection = CASE WHEN qvi_inf.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                             ELSE 0
                        END ,
        qvi_AdverseEffects = CASE WHEN qvi_adv.HSP_ACCOUNT_ID IS NOT NULL
                                  THEN 1
                                  ELSE 0
                             END ,
        qvi_FallsTrauma = CASE WHEN qvi_falls.HSP_ACCOUNT_ID IS NOT NULL
                               THEN 1
                               ELSE 0
                          END ,
        qvi_ForeignObjectRetained = CASE WHEN qvi_forobject.HSP_ACCOUNT_ID IS NOT NULL
                                         THEN 1
                                         ELSE 0
                                    END ,
        qvi_PerforationLaceration = CASE WHEN qvi_perf.HSP_ACCOUNT_ID IS NOT NULL
                                         THEN 1
                                         ELSE 0
                                    END ,
        qvi_DVTPTE = CASE WHEN qvi_dvtpte.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                          ELSE 0
                     END ,
        qvi_Pneumonia = CASE WHEN qvi_pne.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                             ELSE 0
                        END ,
        qvi_pneasp = CASE WHEN qvi_pneasp.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                          ELSE 0
                     END ,
        qvi_pnevent = CASE WHEN qvi_pnevent.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                           ELSE 0
                      END ,
        qvi_Shock = CASE WHEN qvi_shock.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                         ELSE 0
                    END ,

		qvi_thriat =  CASE WHEN qvi_thriat.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                         ELSE 0
                    END,

	qvi_thrpulm=  CASE WHEN qvi_thrpulm.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                         ELSE 0
                    END,
        qvi_Any = CASE WHEN qvi_any.HSP_ACCOUNT_ID IS NOT NULL THEN 1
                       ELSE 0
                  END
FROM    RADB.dbo.CRD_ERAS_EncDim_GHGI AS f 


LEFT JOIN (SELECT AdmissionCSN,SUM(CASE WHEN ErasCase='Eras Case' THEN 1 ELSE 0 END) AS erascount

					FROM radb.dbo.vw_CRD_ERAS_Case AS vcec
					GROUP BY AdmissionCSN
		  ) AS erasflag ON erasflag.AdmissionCSN=f.pat_enc_csn_id
		  --QVI infection  
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 17, 18, 19 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_inf ON f.HSP_ACCOUNT_ID = qvi_inf.HSP_ACCOUNT_ID        
  
--adverse effects  
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 28 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_adv ON f.HSP_ACCOUNT_ID = qvi_adv.HSP_ACCOUNT_ID       
          
 --falls and trauma         
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 7 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_falls ON f.HSP_ACCOUNT_ID = qvi_falls.HSP_ACCOUNT_ID       
  
--foreign object retained  
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                      LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 3 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_forobject ON f.HSP_ACCOUNT_ID = qvi_forobject.HSP_ACCOUNT_ID       
          
   --thrombosis /embolism
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 9 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_dvtpte ON f.HSP_ACCOUNT_ID = qvi_dvtpte.HSP_ACCOUNT_ID       
  
    
--perforations and lacerations            
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 4 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_perf ON f.HSP_ACCOUNT_ID = qvi_perf.HSP_ACCOUNT_ID            
  
--pneumonia        
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 11, 12 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_pne ON f.HSP_ACCOUNT_ID = qvi_pne.HSP_ACCOUNT_ID           

--pneumonia ventilator assoc
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Hierarchy_Key = 52
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_pnevent ON f.HSP_ACCOUNT_ID = qvi_pnevent.HSP_ACCOUNT_ID           


--pneumonia aspiration
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Hierarchy_Key = 51
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_pneasp ON f.HSP_ACCOUNT_ID = qvi_pneasp.HSP_ACCOUNT_ID           
	
  
--shock        
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Num IN ( 16 )
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_shock ON f.HSP_ACCOUNT_ID = qvi_shock.HSP_ACCOUNT_ID        
  
--any qvi        
        LEFT JOIN ( SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                    GROUP BY f.HSP_ACCOUNT_ID
                  ) qvi_any ON f.HSP_ACCOUNT_ID = qvi_any.HSP_ACCOUNT_ID      
          
		  

--Thrombosis/Embolism: Pulmonary: Iatrogenic Condition
		LEFT JOIN (SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Hierarchy_Key IN ( 61 )
                    GROUP BY f.HSP_ACCOUNT_ID
				   ) AS qvi_thriat ON qvi_thriat.HSP_ACCOUNT_ID = f.HSP_ACCOUNT_ID



--Thrombosis/Embolism: Pulmonary: Pulmonary
		LEFT JOIN (SELECT  f.HSP_ACCOUNT_ID
                    FROM    RADB.dbo.QVI_Fact f
                            LEFT JOIN RADB.dbo.QVI_Hierarchy_Dim AS d ON f.QVI_Hierarchy_Key = d.QVI_Hierarchy_Key
                    WHERE   d.QVI_Hierarchy_Key IN ( 60 )
                    GROUP BY f.HSP_ACCOUNT_ID
				   ) AS qvi_thrpulm ON qvi_thriat.HSP_ACCOUNT_ID = f.HSP_ACCOUNT_ID









