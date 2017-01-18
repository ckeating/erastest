--encounter fact
RADB.dbo.CRD_ERAS_GHGI_EncDim

--case fact
radb.dbo.CRD_ERAS_GHGI_Case


--metric dim
 radb.dbo.CRD_ERAS_GHGI_MetricDim AS m

 --metric fact
 radb.dbo.CRD_ERAS_GHGI_MetricFact 


--report fact
SELECT MetricName
,      MetricID
,      MetricCalculation
,      MetricType
,      TrendOrd
,      full_date
,      week_begin_date
,      ERASRptGrouper
,	   SurgeonName
,	   OpenVsLaparoscopic
,      Orderset
,      Num
,      Den 
FROM radb.dbo.vw_CRD_ERAS_GHGI_Report

sp_helptext vw_CRD_ERAS_GHGI_Report

