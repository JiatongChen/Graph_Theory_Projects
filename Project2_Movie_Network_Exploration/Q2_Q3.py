import pandas as pd
import numpy as np
from itertools import islice
from igraph import *
import igraph

def take(n, iterable):
	return list(islice(iterable, n))
def weight(count1,name1):
	indi_count=count_dict[name1]
	return float(count1)/indi_count
def pgOfName(name,df,pg):
	index=np.where(df["name"]==name)[0]
	if (len(index)>0):
		return pg[index[0]]
individual_count=pd.read_csv("individual_count.csv",header=None,names=["name","count"])
count_dict=dict(zip(individual_count["name"],individual_count["count"]))

pairs_count=pd.read_csv("pairs_count.csv",header=None)
pairs_count.columns=["name1","name2","count"]
pairs_count['edge1']=pairs_count.apply(lambda row: weight(row['count'],row['name1']),axis=1)
pairs_count['edge2']=pairs_count.apply(lambda row: weight(row['count'],row['name2']),axis=1)

edge_list1=pairs_count[["name1","name2","edge1"]]
edge_list2=pairs_count[["name2","name1","edge2"]]
edge_list2.columns=["name1","name2","edge1"]
edge_list=pd.concat([edge_list1,edge_list2],axis=0)
edge_list.columns=["name1","name2","weight"]
print "edge list generated\n"
individual_count['id']=range(1,len(individual_count)+1)
actor_id_dict=dict(zip(individual_count["name"],individual_count["id"]))
edge_list["id1"]=edge_list["name1"].map(lambda x:actor_id_dict[x])
edge_list["id2"]=edge_list["name2"].map(lambda x:actor_id_dict[x])
actor_graph=edge_list[["id1","id2","weight"]]
actor_graph.to_csv("actor_graph.csv",sep=" ",header=False,index=False)

ig=read("actor_graph.csv",format="ncol",weights=True,directed=True)
pg=ig.pagerank()

k=10
top_k_indices=np.argsort(pg)[-k:]
name_value=zip(individual_count["name"][top_k_indices],[pg[x] for x in top_k_indices])

deg=ig.degree(mode="IN")
import matplotlib.pyplot as plt

fig=plt.figure()
plt.scatter(deg,pg,s=3)
plt.ylim(0,1e-4)

plt.xlabel("IN degree of vertices")
plt.ylabel("Pagerank")
plt.title("Pagerank vs Vertices degree")
plt.savefig("fig1.png")
plt.show()
