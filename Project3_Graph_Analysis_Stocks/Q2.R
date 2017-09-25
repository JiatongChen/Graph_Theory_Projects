library(igraph)
readkey <- function()
{
    cat ("Press [enter] to continue")
    line <- readline()
}

weight=apply(cov_matrix,1:2,function(x) 2*sqrt(1-x))
ig=graph_from_adjacency_matrix(weight,mode="upper",weighted=TRUE,diag=T)
hist(E(ig)$weight,main="edge weights",xlab=expression("d"["ij"]),ylab="frequency")
readkey()

rest_files=lapply(file_names,function(x) gsub(".*/(.*)\\.csv","\\1",x))
sectors=name_sector$Sector[which(name_sector$Symbol %in% rest_files)]

span_tree=mst(ig,weights=E(ig)$weight)
groups_list=lapply(unique(sectors), function(x) which(sectors %in% x))
#print(groups_list[1])

plot.igraph(span_tree,vertex.size=5,vertex.label=NA,edge.width=E(span_tree)$weight,vertex.color=rainbow(length(sectors))[rank(sectors)],vertex.color="SkyBlue2")
