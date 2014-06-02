library(RPostgreSQL)
library(StatMatch)
sessionInfo()

## configuration de la connection à PG
dbDisconnect(con)
dbUnloadDriver(m)

m <- dbDriver("PostgreSQL")
con <- dbConnect(m, user= "postgres", password="postgres", dbname="postgis21",  port=5434)
##dbDisconnect(con)
## don : table des donneurs
don <- dbGetQuery(con,"select agerevq, cs1, derou, dipl, empl, immi, 
inatc , lprm , moco, na5  , nperr, sexe , statr,  stocd, tp , trans, typl, typmr, voit, ident
from fusion.fd_ind where idgeo=2 ; ")

## rec : table des receveurs
rec <- dbGetQuery(con,"select agerevq, cs1, derou, dipl, empl, immi, 
inatc , lprm , moco, na5  , nperr, sexe , statr,  stocd, tp , trans, typl, typmr, voit, ident
from fusion.fd_mobpro where idgeo=2 ; ")
don <- data.frame(lapply(don[,1:19],factor))
rec <- data.frame(lapply(rec[,1:19],factor))

rownames(rec) <- rec$ident
rownames(don) <- don$ident
str(don)
out <- NND.hotdeck(data.rec=rec, data.don=don,
            match.vars=c(  "agerevq", "cs1", "derou", "dipl", "empl", "immi", 
                           "inatc" , "lprm" , "moco", "na5"  , "nperr", "sexe" , "statr",  "stocd", "tp" , "trans", "typl", "typmr", "voit"))


res <- data.frame(cbind(out$mtc.ids,out$dist.rd))

dbWriteTable(con,"fusion.res",res)