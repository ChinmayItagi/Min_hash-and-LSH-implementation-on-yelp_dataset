import json
import time
import pyspark
import sys

if __name__== '__main__':
    sc = pyspark.SparkContext()

    df=sc.textFile("/home/chinmay/Gitfiles/Min_hash and LSH implementation on yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_train.csv")
    header = df.first()

    df = df.filter(lambda x: x != header).map(lambda x: x.split(",")).cache()
    df_business_id = df.map(lambda x:x[1]).distinct().collect()
    df_business_user = df.map(lambda x:x[0]).distinct().collect()
    df = df.map(lambda x:(x[1],x[0])).groupByKey().mapValues(set).take(1)



    dict_business = {}
    for i,v in enumerate(df_business_id,1):
        dict_business[v] = i
    print(dict_business)



    dict_business = {}
    for i,v in enumerate(df_business_id,1):
        dict_business[v] = i
    print(dict_business)
