USE Clarity

IF object_id('tempdb..##Flow') IS NOT NULL
	DROP TABLE ##Flow;

WITH baseq AS (
  		
SELECT  
		ids.EPT_CSN AS pat_enc_csn_id
       ,ifm.FSD_ID
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

FROM    clarity.dbo.IP_DATA_STORE AS ids
		--clarity.dbo.pat_enc_hsp AS ids
		--JOIN radb.dbo.CRD_ERAS_case   b ON ids.EPT_CSN = b.admissioncsn
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
		AND ids.EPT_CSN=126200597
)
SELECT 
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
       LDA_Properties 
       
INTO    ##Flow
FROM baseq
ORDER BY pat_enc_csn_id,recorded_time;


SELECT * 
FROM ##Flow AS f
ORDER BY Flowsheet_DisplayName,RECORDED_TIME