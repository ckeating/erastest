--location table

SELECT * FROM dbo.CRD_ERASProject_Dim AS cepd



DROP TABLE dbo.CRD_ERASProject_Dim
CREATE TABLE dbo.CRD_ERASProject_Dim
(ProjectID INT IDENTITY NOT NULL,
 DeliveryNetwork VARCHAR(50),
 DeliveryNetwork_ShortName VARCHAR(25),
 ProjectName VARCHAR(100),
 ProjectShortName VARCHAR(25)
 )
ALTER TABLE dbo.CRD_ERASProject_Dim
ADD CONSTRAINT pk_CRD_ERASProject_Dim PRIMARY KEY (ProjectID)

sp_help CRD_ERASProject_Dim

--additional for GH
44140 (Colectomy, partial; with anastomosis)
44320  (Colostomy)
44227  (Laparaoscopy, surgical, closure of enterostomy, large or small intestine, with resection 
-	44238
--cpt dim
SELECT * FROM radb.dbo.CRD_ERAS_CPT_Dim AS cecd

sP_help CRD_ERAS_CPT_Dim

ALTER TABLE radb.dbo.CRD_ERAS_CPT_Dim
ADD CONSTRAINT PK_CRD_ERAS_CPT_Dim PRIMARY KEY NONCLUSTERED(DeliveryNetwork,ERASProject,CPTCode)

ALTER TABLE radb.dbo.CRD_ERAS_CPT_Dim
ALTER COLUMN CPTCode VARCHAR(25) NOT NULL



AND clarity.dbo.OR_PROC_CPT_ID.REAL_CPT_CODE  IN  
      ('44144','44145','44146','44147','44150','44156','44160','44204',
'44205','44206','44207','44208','44210','44211','44212','44620','44625','44626',
'45110','45111','45112','45113','45114','45116','45119','45120','45121','45123','45126','45130','45160','45550','44157','44158')



SELECT COUNT(*),COUNT(DISTINCT proc_code) 
FROM clarity.dbo.CLARITY_EAP AS ce
WHERE PROC_CODE IN  
      ('44144','44145','44146','44147','44150','44156','44160','44204',
'44205','44206','44207','44208','44210','44211','44212','44620','44625','44626',
'45110','45111','45112','45113','45114','45116','45119','45120','45121','45123','45126','45130','45160','45550','44157','44158')


INSERT radb.dbo.CRD_ERAS_CPT_Dim
        ( DeliveryNetwork ,
          ERASProject ,
          CPTCode ,
          ProcedureCategory ,
          CPT_Description
        )
SELECT 'GH'
,'GI'
, ce.PROC_CODE
, '*Unknown CPT category'
, ce.PROC_NAME
FROM clarity.dbo.CLARITY_EAP AS ce
WHERE ce.PROC_CODE IN  
      ('44144','44145','44146','44147','44150','44156','44160','44204',
'44205','44206','44207','44208','44210','44211','44212','44620','44625','44626',
'45110','45111','45112','45113','45114','45116','45119','45120','45121','45123','45126','45130','45160','45550','44157','44158')



INSERT radb.dbo.CRD_ERAS_CPT_Dim
        ( DeliveryNetwork ,
          ERASProject ,
          CPTCode ,
          ProcedureCategory ,
          CPT_Description
        )
SELECT 'GH'
,'GI'
, ce.PROC_CODE
, '*Unknown CPT category'
, ce.PROC_NAME
FROM clarity.dbo.CLARITY_EAP AS ce
WHERE ce.PROC_CODE IN  
      ('44140','44320','44227','44238')

44140 (Colectomy, partial; with anastomosis)
44320  (Colostomy)
44227  (Laparaoscopy, surgical, closure of enterostomy, large or small intestine, with resection 
-	44238


SELECT * FROM radb.dbo.CRD_ERAS_CPT_Dim
WHERE DeliveryNetwork='GH' AND ERASProject='GI'
ORDER BY CPTCode



--add another for BH
INSERT radb.dbo.CRD_ERAS_CPT_Dim
        ( DeliveryNetwork ,
          ERASProject ,
          CPTCode ,
          ProcedureCategory ,
          CPT_Description
        )
SELECT 'BH'
,'GYN'
, ce.PROC_CODE
, '*Unknown CPT category'
, ce.PROC_NAME
FROM clarity.dbo.CLARITY_EAP AS ce
WHERE ce.PROC_CODE IN  
      ('58548')