a<-ls()
rm(all)
a<-ls()
library(RPostgreSQL)
library(StatMatch)
library(ade4)
sessionInfo()
getwd()
setwd("D:/R/work")
## configuration de la connection à PG
##dbDisconnect(con)
##dbUnloadDriver(m)

m <- dbDriver("PostgreSQL")
con <- dbConnect(m, user= "postgres", password="postgres", dbname="postgis21",  port=5434)
##dbDisconnect(con)
## don : table des donneurs


for (i in 1:1919) { ## recommencer a partir de 1088. supprimer 1088 sur la table pg
  print(i)
  
  query <- paste0("select agerevq, cs1, derou, dipl, empl, immi,inatc , lprm , moco, na5  , nperr, sexe , statr,  stocd, tp , trans, typl, typmr, voit, 1 as t, 'I'||ident as id, iris from fusion.fd_ind where idgeo=", i ," union select agerevq, cs1, derou, dipl, empl, immi, inatc , lprm , moco, na5  , nperr, sexe , statr,  stocd, tp , trans, typl, typmr, voit, 2 as t, 'M'||ident as id, 'XX' as iris from fusion.fd_mobpro where idgeo=", i ," ; ")
  
  fic <- dbGetQuery(con,query)
  
  identifiant <- fic$id ## identifiant des obs
  
  fic <- data.frame(lapply(fic[,1:22],factor)) ## transfo en facteur de toutes les variables
  rownames(fic) <- identifiant
  
  afcm <- dudi.acm(df=fic[,1:19],scannf= FALSE,nf=100) ## acm sur les var agerev > voit
  
  coord <- afcm$li ##rownames est conserve
  
  
  rec <- subset(coord, substr(rownames(coord),1,1)=='I') ##receveur  
  don <- subset(coord, substr(rownames(coord),1,1)=='M') ##donneur
  
  ## ajout d un id de 1000 en 1000
  rec <- cbind(rec, 'id'= (c(1:nrow(rec)))) ## creation variable idex de 1 a nrow 
  rec$id <- floor(rec$id / 1000) ## creation de 0 a xx
  maxid <- max(rec$id)
  ## boucle sur les paquets de 1000
  for (j in 0:maxid) {
    print(j)
    out <- NND.hotdeck(data.rec=subset(rec,id==j), data.don=don, dist.fun="Euclidean",
                       match.vars=colnames(don))
    res <- data.frame(cbind(out$mtc.ids,out$dist.rd,i,j)) ## on stocke aussi i et j 
    dbWriteTable(con, "resfusion1000", res, append = T)
    rm(out, res) ## suppression fichiers
  }
  rm(coord, afcm, fic, rec, don) ## suppression fichiers
}