import json
import time
import pyspark
import sys
import random
import itertools

def generate_candidates(iterator):
    chunk = list(iterator)
    column_dict = {}

    candidate_pairs = []

    zipped_columns = zip(*[chunk[i][1] for i in range(len(chunk))])


    for key, column in zip(dict_business.values(), zipped_columns):
        column_dict[key] = column
    actual_competing_pairs = competing_pairs-removing_pairs

    for combination in actual_competing_pairs:
        if column_dict[combination[0]] == column_dict[combination[1]]:
            candidate_pairs.append(combination)
            removing_pairs.add(combination)

    yield candidate_pairs

def mapping(tup):
    user_id=dict_user[tup[0]]
    business_id = dict_business[tup[1]]
    return (business_id,user_id)


def minhash(value):
	hashvalue=[]
	for i in range(50):
		a1 = a[i]
		b1 = b[i]
		temp_hash_cal = []
		#hashvalue = []
		for j in value[1]:
			val = ((a1*j)+b1)%number_of_users
			temp_hash_cal.append(val)
		min_hash_val = min(temp_hash_cal)
		yield ((i,value[0]),min_hash_val)




def hash_list(value): # input is indexof business_id , hash value of that business , this func is called foreach hash function
	v = list(value)
	h_list=[0 for i in range(len(v))]
	h_list.append(0)
	#print("--------", len(value))
	#print("--------", len(h_list))
	#print(h_list)
	#print(len(v))
	for v1 in v:
		#print("v1",v1[0])
		#print("v2",v1[1][0])
		h_list[v1[0]] = v1[1][0]
		#s.append(v1[0])



		#h_list[v1[0]]=v1[1][0] # each hash function ==> hash values with the index(h_list)
	#print(h_list)
	return h_list




if __name__== '__main__':
	start = time.time()
	sc = pyspark.SparkContext()


	df=sc.textFile("/home/chinmay/Gitfiles/Min_hash_and_LSH_implementation_on_yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_train.csv")
	# header in order user_id,business_id,stars
	header = df.first()

	removing_pairs = set()
	df = df.filter(lambda x: x != header).map(lambda x: x.split(",")).cache()
	df_business_id = df.map(lambda x:x[1]).distinct().collect()
	df_business_user = df.map(lambda x:x[0]).distinct().collect()
	
	
	end = time.time()
	print(end-start)
	# mapping of the user ids and business_id to intergers

	dict_business = {}
	for i,v in enumerate(df_business_id,1):
		dict_business[v] = i
	
	dict_user = {}
	for i,v in enumerate(df_business_user,1):
		dict_user[v] = i
	


	number_of_users  = len(dict_user)
	#print(number_of_users)

	a = random.sample(range(1,1000),100)
	b = random.sample(range(1,1000),100)

	df = df.map(mapping).groupByKey().mapValues(lambda x:sorted(x))
	df_min_hash  = df.map(minhash).flatMap(list).groupByKey().map(lambda x:(x[0][0],[x[0][1],list(x[1])])).groupByKey().map(lambda x:(x[0],hash_list(x[1]))).collect()
	candi = sc.parallelize(df_min_hash,13)
	#print(candi.collect())
	acompeting_pairs = set(itertools.combinations(dict_business.values(), 2))

	candidate = candi.mapPartitions(generate_candidates).flatMap(lambda x: x).distinct().collect()
	#map(lambda x: (x[0],hash_list(x[1]))).collect()# check of the values
	#703, 5683, 11122, 7146, 4894, 3946, 8117, 10196, 5451, 10757
	#df_check = df_min_hash.map(lambda x:(x[0],len(x[1]))).collect()




	# coll = df_min_hash.collect()
	# sig_dict = {}





	# for i in coll:
	# 	sig_dict[i[0]]=i[1]



	# for comb in itertools.combinations(dict_business.values,2):
	#for i in df_min_hash:




	print(candidate)
	print(len(candidate))
	end = time.time()
	print("time_taken",end-start)


	#[69, 1730, 797, 937, 1187, 809, 457, 908, 228, 2