from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import pyspark
import  math
from collections import OrderedDict
import time
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel,Rating





if __name__== '__main__':
    start = time.time()
    sc = pyspark.SparkContext()


    df_train=sc.textFile("/home/chinmay/Gitfiles/Min_hash_and_LSH_implementation_on_yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_train.csv")
    df_test=sc.textFile("/home/chinmay/Gitfiles/Min_hash_and_LSH_implementation_on_yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_val.csv")

    header = df_test.first()
    df_train = df_train.filter(lambda x: x != header).map(lambda x:x.split(","))
    df_test= df.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).collect()

    print(df_test)



#
# df_business_id = df.map(lambda x:x[1]).distinct().collect()
# df_business_user = df.map(lambda x:x[0]).distinct().collect()
#
#
#
# test_ = df_test.map(lambda x:x.split(",")).map(lambda x:(x[0],x[1]))






#
#header in order user_id,business_id,stars
