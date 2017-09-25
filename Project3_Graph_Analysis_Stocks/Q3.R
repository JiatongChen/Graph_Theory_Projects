library(igraph)

 neigh=lapply(V(span_tree),function(x) neighbors(span_tree,x,mode="all"))
 neigh_size=lapply(neigh,length)

neigh_sector_size=lapply(1:length(V(span_tree)),function(x) length(which(sectors[unlist(neigh[x])]==sectors[x])))

 neigh_size=as.numeric(neigh_size)
 neigh_sector_size=as.numeric(neigh_sector_size)
 alpha0=neigh_sector_size/neigh_size
 alpha=mean(alpha0)

cat(alpha,"\n")
set.seed(1)
proba=lapply(groups_list,function(x) length(x)/length(sectors))
random_sectors=sample(unique(sectors),length(sectors),replace=T,prob=proba)

neigh_sector_size=lapply(1:length(V(span_tree)),function(x) length(which(random_sectors[unlist(neigh[x])]==random_sectors[x])))
neigh_size=as.numeric(neigh_size)
neigh_sector_size=as.numeric(neigh_sector_size)
alpha0=neigh_sector_size/neigh_size
alpha=mean(alpha0)
print(alpha)
