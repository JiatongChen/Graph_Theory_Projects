import pandas as pd
import numpy as np
import glob
import igraph
from igraph import *
import re

name_sector=pd.read_csv("Name_sector.csv",header=0)
name_dict=dict(zip(name_sector["Symbol"],name_sector["Sector"]))
filenames=glob.glob('data/*.csv')
labels=[re.findall(".*/(.*)\\.csv",x) for x in filenames]


csvfiles=[pd.read_csv(f,header=0) for f in filenames]

log_diff_list=[np.diff(np.log(df["Close"])) for df in csvfiles]

log_diff_arr=np.asarray(log_diff_list)

cor_matrix=np.corrcoef(log_diff_arr)

weight_matrix=np.sqrt(2*(1-cor_matrix))
for i in range(0,weight_matrix.shape[0]):
	weight_matrix[i,i]=0
ig=Graph.Adjacency((weight_matrix>0).tolist(),mode=ADJ_UPPER)

ig.es['weight']=weight_matrix[weight_matrix.nonzero()]

ig.vs['label']=labels
#duplicate min_span_tree edges

min_span_tree=ig.spanning_tree(weights=ig.es['weight'])
min_span_tree.to_directed(mutual=True)

#find Euler tour
raw_input("find Euler Tour")
def find_Euler_tour(Graph):
	stack=[]
	e_tour=[]
	def dfs(start):
		stack.append(start)
		
		conn=False;
		for v in Graph.neighbors(start):
			if Graph.are_connected(start,v):
				Graph.delete_edges([(start,v)])
				dfs(v)
				
			break
		
	def isBridge(u):
		if (Graph.degree(u,mode="OUT")==0):
			return True
		else:
			return False
	def printEulerUtil(u):
		stack.append(u)
		while (len(stack)>0):
			if (isBridge(stack[-1])):
				e_tour.append(stack.pop())
			else:
				dfs(stack.pop())
	start=0
	deg=Graph.degree()
	for i in range(len(deg)):
            if deg[i] %2 != 0 :
                start = i
                break
	
	printEulerUtil(4)
	e_tour.reverse()
	return e_tour
e_tour=find_Euler_tour(min_span_tree)
#find TSP solution
def shortcut_Euler(e_tour):
	ret=[]
	cut=False
	for v in e_tour:
		if v in ret:
			cut=True
			continue
		else:
			ret.append(v)
	return shortcut_Euler(ret) if (cut) else ret

TSP_solution=shortcut_Euler(e_tour)			


