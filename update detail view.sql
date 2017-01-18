SELECT CSN
,      HAR
,      PatientName
,      MRN
,      Admission_DTTM
,      Discharge_DTTM
,      LOSDays
,      LOSHours
,      Enc_DischargeDisposition
,      PatientStatus
,      BaseClass
,      Enc_Pat_Class
,		[ERAS OrderSet Used?]
,      [Admission Type]
,      Log_ID
,      ProcedureDisplayName
,      ErasCase
,      CPT_Code
,      SurgeryDate
,      ProcedureType
,      Surgery_Patient_Class
,      LogStatus
,      ProcedureName
,      SurgeonName
,      SurgeonRole
,      [Surg Log Class]
,      NumOfPanels
,      AnesCSN
,      AdmissionCSN
,      SurgicalCSN
,      Surgery_Room_Name
,      ALL_PROCS_PANEL
,      procline
,      SurgeryServiceName
,      Sched_Start_Time
,      SurgeryLocation
,      setupstart
,      setupend
,      inroom
,      outofroom
,      cleanupstart
,      cleanupend
,      inpacu
,      outofpacu
,      inpreprocedure
,      outofpreprocedure
,      anesstart
,      anesfinish
,      procedurestart
,      procedurefinish
,      postopday1_begin
,      postopday2_begin
,      postopday3_begin
,      postopday4_begin
,      CaseLength_min
,      CaseLength_hrs
,      timeinpacu_min
,      preadm_counseling
,      [Received pre admission counseling?]
,      TemperatureInPacu
,      [Normal temp on arrival to PACU?]
,      NormalTempInPacu
,      [Ambulate POD0?]
,      ambulatepod0
,      clearliquids_3ind
,      [Clear liq 3 hrs before induction?]
,      clearliquids_pod0
,      [Clear liq given POD0?]
,      ambulate_pod1
,      [Ambulate POD1?]
,      solidfood_pod2
,      [Solid food POD2?]
,      ambulate_pod2
,      [Ambulate POD2?]
,      hrs_toleratediet
,      Admission_DT
,      Discharge_DT
,      HospitalWide_30DayReadmission_NUM
,      HospitalWide_30DayReadmission_DEN
,      NumberofProcs
,      qvi_Infection
,      qvi_AdverseEffects
,      qvi_FallsTrauma
,      qvi_ForeignObjectRetained
,      qvi_PerforationLaceration
,      qvi_DVTPTE
,      qvi_Pneumonia
,      qvi_Shock
,      qvi_Any 
FROM radb.dbo.vw_CRD_ERAS_GHGI_Report_Detail




SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO
ALTER VIEW dbo.vw_CRD_ERAS_GHGI_Report_Detail
as
SELECT  e.CSN
,       e.HAR
,       e.PatientName
,       e.MRN
,       e.Admission_DTTM
,       e.Discharge_DTTM
,       e.Admission_DT
,       e.Discharge_DT
,       e.LOSDays
,       e.LOSHours
,       e.Enc_DischargeDisposition
,       e.PatientStatus
,       e.BaseClass
,       e.Enc_Pat_Class
,       e.[Admission Type]
,       CASE WHEN e.OrdersetFlag = 1 THEN 'Yes'
             ELSE 'No'
        END AS [ERAS OrderSet Used?]
,       c.Log_ID
,       c.ProcedureDisplayName
,       c.ErasCase
,       c.CPT_Code
,       c.SurgeryDate
,       c.ProcedureType
,       c.Surgery_Patient_Class
,       c.LogStatus
,       c.ProcedureName
,       c.SurgeonName
,       c.SurgeonRole
,       [Surg Log Class] = ISNULL(c.CASECLASS_DESCR,
                                  '*Unknown surgical log class')
,       c.NUM_OF_PANELS AS NumOfPanels
,       c.AnesCSN
,       c.AdmissionCSN
,       c.SurgicalCSN
,       c.Surgery_Room_Name
,       c.ALL_PROCS_PANEL
,       c.procline
,       c.SurgeryServiceName
,       c.Sched_Start_Time
,       c.SurgeryLocation
,       c.setupstart
,       c.setupend
,       c.inroom
,       c.outofroom
,       c.cleanupstart
,       c.cleanupend
,       c.inpacu
,       c.outofpacu
,       c.inpreprocedure
,       c.outofpreprocedure
,       c.anesstart
,       c.anesfinish
,       c.procedurestart
,       c.procedurefinish
,       c.postopday1_begin
,       c.postopday2_begin
,       c.postopday3_begin
,       c.postopday4_begin
,       c.CaseLength_min
,       c.CaseLength_hrs
,       c.timeinpacu_min
,        
		--process metrics
        c.TemperatureInPacu
,       c.NormalTempInPacu
,       c.ambulatepod0
,       c.ambulate_pod1
,       c.clearliquids_3ind
,       c.clearliquids_pod0
,       c.clearliquids_pod1
,       c.solidfood_pod2
,       c.ambulate_pod2
,       c.hrs_toleratediet
,       c.hrs_tobowelfunction
,       c.lumbar_epi
,       c.goal_guidelines
,       c.mm_antiemetic_intraop
,       c.hrs_last_IVpainmed
,       c.tapblock_placed_flag
,       c.postop_painiv_count
,       c.postop_paintotal_count
,       c.log_orderset_flag
,       e.HospitalWide_30DayReadmission_NUM
,       e.HospitalWide_30DayReadmission_DEN
,       e.NumberofProcs
,       e.qvi_Infection
,       e.qvi_AdverseEffects
,       e.qvi_FallsTrauma
,       e.qvi_ForeignObjectRetained
,       e.qvi_PerforationLaceration
,       e.qvi_DVTPTE
,       e.qvi_Pneumonia
,       e.qvi_Shock
,       e.qvi_Any
FROM    RADB.dbo.vw_CRD_ERAS_GHGI_EncDim AS e
        LEFT JOIN RADB.dbo.vw_CRD_ERAS_GHGI_Case AS c ON c.AdmissionCSN = e.CSN;








SELECT *
FROM radb.dbo.vw_CRD_ERAS_GHGI_Report_Detail

