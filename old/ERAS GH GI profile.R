setwd("\\ynhh\root\Shared2\DSSFILES\keating\Projects\.ERAS GH")
library(RODBC)
ch<-odbcConnect(dsn="Clarity")
qry<-"Select * from RADB.dbo.CRD_ERAS_Case_GHGI"
tst<-sqlQuery(ch,qry)

encqry<- "SELECT * FROM RADB.dbo.CRD_ERAS_EncDim_GHGI"
enc=sqlQuery(ch,encqry)
str(tst)
summary(tst)

pdf("encounter.pdf")
summary(enc)
dev.off()


table(tst$)

View(table(tst$Surgery_Patient_Class))

#describe(tst)

table(c(tst$pat_enc_csn_id))

length(unique(tst$pat_enc_csn_id))

boxplot(tst$LOS_days,horizontal = TRUE)
plot(tst$LOS_days,tst$pat_enc_csn_id)

los<-cut(tst$LOS_days,breaks=seq(0,100,by=20))
hist(los)
table(los)

par(mfrow=c(1,3), mar=c(3,3,3,3))
hist(tst$LOS_days, main="NBA Player Heights", xlab="inches") 

histinfo=hist(tst$LOS_days, main="NBA Player Heights", xlab="inches") 

histinfo

summary(los)

hist(tst$LOS_days,freq=FALSE)

#rug
d=density(tst$LOS_days)
plot(d)
polygon(d,col="lightgray",border="gray")
rug(tst$LOS_days,col="red")

install.packages("beanplot")
#violin plot
library(vioplot)
vioplot(tst$LOS_days, horizontal=TRUE, col="gray")

#beanplot
library(beanplot)
beanplot(tst$LOS_days)


summary(tstfact)
install.packages("psych")
library(psych)
describeBy(tst$AttendingProvider)
hist(tst$ct ~tst$wkflag)
plot(tst$dtonly,tst$ct,col=wkflag)
chedw<-odbcConnect(dsn="edw")
tst<-sqlQuery(chedw,qry)
describe(tst)
length(tst)
hist(tst$AttendingProvider)



mdstaff <- read.csv(file="c:/mdstaff.csv", header=TRUE, sep=",")
con<-file("c:/test.log")
sink(con,append=TRUE)

summary(mdstaff,maxsum=100)
describe(mdstaff)

