import numpy as np
import pandas as pd
import re
from pyspark import SparkContext
from igraph import *
import igraph
import heapq
#sc=SparkContext()

def sub_name(movie):
	
	pat="(.*\([^a-zA-Z]{4}\))"
	ret=re.findall(pat,movie) 
	return ret[0] if len(ret)>0 else movie

def movie_name_pairs(filename1,filename2):
	actor_content=sc.textFile(filename1)
	actor_movies=actor_content.map(lambda x:[strs.encode("utf-8").decode("unicode_escape") for strs in x.split("\t\t")]).map(lambda x: [x[0]]+[sub_name(movie) for movie in x[1:]])
	actress_content=sc.textFile(filename2)
	actress_movies=actress_content.map(lambda x:[strs.encode("utf-8").decode("unicode_escape") for strs in x.split("\t\t")]).map(lambda x: [x[0]]+[sub_name(movie) for movie in x[1:]])
	name_movies=actor_movies.union(actress_movies)
	names=name_movies.map(lambda x: x[0]).distinct().collect()
	movies=name_movies.flatMap(lambda x:x[1:]).distinct().collect()
	name_dict=dict(zip(names,range(0,len(names))))
	movie_dict=dict(zip(movies,range(len(names),len(names)+len(movies))))
	nm_pairs=name_movies.flatMap(lambda x: [(name_dict[x[0]],movie_dict[movie]) for movie in x[1:]]).collect()
	return nm_pairs,name_dict,movie_dict

nm_pairs,name_dict,movie_dict=movie_name_pairs("actor_movies.txt","actress_movies.txt")
	
vertices=[0]*len(name_dict)+[1]*len(movie_dict)
g=Graph.Bipartite(vertices,nm_pairs)

movie_rating=pd.read_csv("movie_rating.txt",sep="\t",header=None,names=["movie","drop","rating"])
movie_rating=movie_rating.drop("drop",axis=1)
movie_rating["newname"]=movie_rating["movie"].apply(lambda x:sub_name(x.decode("unicode_escape")))

movie_rating["id"]=movie_rating["newname"].apply(lambda x:movie_dict.get(x))

def find_rating(id):
	
	ret=movie_rating[movie_rating["id"]==id].rating
	return ret.item() if (len(ret)>0) else 0
def top_3(rating):
	if len(rating)>3:
		return np.mean(heapq.nlargest(3,rating))
	else:
		return np.mean(rating)
actor_metrics=[]
for actor_vertex in name_dict.values():
	neigh=g.neighbors(actor_vertex)
	mv_rating=[find_rating(x) for x in neigh]
	
	avg=np.mean(mv_rating)
	highest=max(mv_rating)
	top3=top_3(mv_rating)
	actor_metrics.append([highest,avg,top3])

actor_metrics=np.asarray(actor_metrics)

movie_name=raw_input("What is the movie name?")
while (movie_name!="None"):
	movie_id=movie_dict.get(sub_name(movie_name))
	neighs=g.neighbors(movie_id)
	high_predict=actor_metrics[neighs,0]
	avg_predict=actor_metrics[neighs,1]
	top3_predict=actor_metrics[neighs,2]
	print "predict according to highest",np.mean(high_predict[np.nonzero(high_predict)]),"\n"
	print "predict according to average",np.mean(avg_predict[np.nonzero(avg_predict)]),"\n"
	print "predict according to top3",np.mean(top3_predict[np.nonzero(top3_predict)]),"\n"
	movie_name=raw_input("What is the movie name?")


