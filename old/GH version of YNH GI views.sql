SELECT 
       CPTCode ,
       ProcedureCategory ,
       CPT_Description 
FROM radb.dbo.CRD_ERAS_CPT_Dim AS cecd
WHERE DeliveryNetwork='gh'


[dbo].[vw_CRD_ERAS_Report_Detail_GHGI]
SELECT * FROM dbo.vw_CRD_ERAS_Report_GH AS vcerg



[dbo].[vw_CRD_ERAS_Case_GH]


SELECT * FROM radb.dbo.vw_CRD_ERAS_Report_Detail

sp_helptext vw_CRD_ERAS_Report


sp_helptext vw_CRD_ERAS_Report_Detail

CREATE VIEW dbo.vw_CRD_ERAS_Report_Detail_GHGI
as
SELECT  c.DeliveryNetwork,

		e.CSN ,
        e.HAR ,
        e.PatientName ,
        e.MRN ,[dbo].[vw_CRD_ERAS_Report_GH]
        e.Admission_DTTM ,
        e.Discharge_DTTM ,
        e.LOSDays ,
        e.LOSHours ,
        e.Enc_DischargeDisposition ,
        e.PatientStatus ,
        e.BaseClass ,
        e.Enc_Pat_Class ,
        e.[Admission Type] ,
        c.Log_ID ,
        c.ProcedureDisplayName ,
        c.ErasCase ,
        c.CPT_Code ,
        c.SurgeryDate ,
        c.ProcedureType ,
        c.Surgery_Patient_Class ,
        c.LogStatus ,
        c.ProcedureName ,
        c.SurgeonName ,
		c.SurgeonRole ,
        [Surg Log Class] = ISNULL(c.CASECLASS_DESCR,
                                  '*Unknown surgical log class') ,
        c.NUM_OF_PANELS AS NumOfPanels ,
        c.AnesCSN ,
        c.AdmissionCSN ,
        c.SurgicalCSN ,
        c.Surgery_Room_Name ,        
        c.ALL_PROCS_PANEL ,
        c.procline ,
        c.SurgeryServiceName ,
        c.Sched_Start_Time ,
        c.SurgeryLocation ,
        c.setupstart ,
        c.setupend ,
        c.inroom ,
        c.outofroom ,
        c.cleanupstart ,
        c.cleanupend ,
        c.inpacu ,
        c.outofpacu ,
        c.inpreprocedure ,
        c.outofpreprocedure ,
        c.anesstart ,
        c.anesfinish ,
        c.procedurestart ,
        c.procedurefinish ,
        c.postopday1_begin ,
        c.postopday2_begin ,
        c.postopday3_begin ,
        c.postopday4_begin ,
        c.CaseLength_min ,
        c.CaseLength_hrs ,
        c.timeinpacu_min ,        
        c.preadm_counseling ,
        c.[Received pre admission counseling?] ,
        c.TemperatureInPacu ,
        c.[Normal temp on arrival to PACU?] ,
        c.NormalTempInPacu ,
        c.[Ambulate POD0?] ,
        c.ambulatepod0 ,
        c.clearliquids_3ind ,
        c.[Clear liq 3 hrs before induction?] ,
        c.clearliquids_pod0 ,
        c.[Clear liq given POD0?] ,
        c.ambulate_pod1 ,
        c.[Ambulate POD1?] ,
        c.solidfood_pod1 ,
        c.[Solid food POD1?] ,
        c.ambulate_pod2 ,
        c.[Ambulate POD2?] ,
        c.hrs_toleratediet ,
        e.Admission_DT ,
        e.Discharge_DT ,
        e.HospitalWide_30DayReadmission_NUM ,
        e.HospitalWide_30DayReadmission_DEN ,
        e.NumberofProcs ,
		e.TotalDirectCost,
        e.qvi_Infection ,
        e.qvi_AdverseEffects ,
        e.qvi_FallsTrauma ,
        e.qvi_ForeignObjectRetained ,
        e.qvi_PerforationLaceration ,
        e.qvi_DVTPTE ,
        e.qvi_Pneumonia ,
        e.qvi_Shock ,
        e.qvi_Any
FROM    RADB.dbo.vw_CRD_ERAS_EncDim AS e
        LEFT JOIN RADB.dbo.vw_CRD_ERAS_Case AS c ON c.AdmissionCSN = e.CSN
		WHERE e.DeliveryNetwork_ShortName='GH'
		AND e.ProjectShortName='GI'

