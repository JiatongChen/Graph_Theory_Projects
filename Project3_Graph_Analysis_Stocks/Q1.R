library(foreach)


readkey <- function()
{
    cat ("Press [enter] to continue")
    line <- readline()
}
#setwd("/media/fang/Files/Study@UCLA/2017/Spring/EE232E/hw4/finance_data")
name_sector=read.csv("Name_sector.csv")
file_list=name_sector[["Symbol"]]
#data_dir=getwd()+"/data"
#setwd(data_dir)
file_names=list.files("data",pattern="*.csv",full.names=T)
ldf=lapply(file_names,read.csv)

ret_value=function(df)
{
data=matrix(unlist(df),ncol=7,byrow=F)
data=data.frame(data)
colnames(data)=c("Date","Open","High","Low","Close","Volume","Adj.Close")
close_price=data[["Close"]]
log(tail(close_price,-1))-log(head(close_price,-1))
}
ret_all=lapply(ldf,ret_value)
cb=combn(length(file_names),2,simplify=T)
df=matrix(unlist(ret_all),nrow=length(file_names),byrow=T)
df=as.data.frame(df)
readkey()

cov_matrix=cor(t(df))

