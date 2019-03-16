from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import pyspark
import  math
from collections import OrderedDict
import time
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel,Rating





if __name__== '__main__':
    start = time.time()
    sc = pyspark.SparkContext()
    #Importing the Dataset

    df_train=sc.textFile("/home/chinmay/Gitfiles/Min_hash_and_LSH_implementation_on_yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_train.csv")
    df_test=sc.textFile("/home/chinmay/Gitfiles/Min_hash_and_LSH_implementation_on_yelp_dataset/Min_hash-and-LSH-implementation-on-yelp_dataset/yelp_val.csv")
    # Mapping it to the integers



    header = df_test.first()
    df_train = df_train.filter(lambda x: x != header).map(lambda x:x.split(",")).cache()
    df_test= df_test.filter(lambda x: x != header).map(lambda x: x.split(",")).cache()

    #user
    df_train_users = df_train.map(lambda x:(x[0])).distinct()
    df_train_users_collect = set(df_train_users.collect())
    df_test_users = df_test.map(lambda x: (x[0])).distinct().collect()

    #bussiness

    df_train_business_id = df_train.map(lambda x: (x[1])).distinct()
    df_train_business_id_collect = set(df_train_business_id.collect())
    df_test_business_id = df_test.map(lambda x: (x[1])).distinct().collect()

    for i in df_test_users:
        df_train_users_collect.add(i)

    for i in df_test_business_id:
        df_train_business_id_collect.add(i)


    dict_user = {}

    for i, v in enumerate(df_train_users_collect, 1):
        dict_user[v] = i


    dict_business = {}

    for i, v in enumerate(df_train_business_id_collect, 1):
        dict_business[v] = i



    actual_rating = df_test.map(lambda x:((dict_user[x[0]],dict_business[x[1]]),float(x[2])))
    # Removing the ratings from the actual_ratinf rdd

    y_rem_pred_data = actual_rating.map(lambda x:(x[0][0],x[0][1]))


    training_data  = df_train.map(lambda x:Rating(dict_user[x[0]],dict_business[x[1]],float(x[2])))
    # Rank and iterations
    rank = 2
    numIterations = 5


    #model Training
    model = ALS.train(training_data, rank, numIterations,lambda_=0.5)

    # Predicting the values
    predicted_values =  model.predictAll(y_rem_pred_data).map(lambda r: ((r[0], r[1]), r[2]))

    #Comparision with actual rating
    comp_rating = actual_rating.join(predicted_values)

    #print(predicted_values.collect())
    # RMSE
    RMSE = (comp_rating.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())**0.5

    inverted__user_dict = dict([[v, k] for k, v in dict_user.items()])
    inverted__business_dict =dict([[v, k] for k, v in dict_business.items()])



    #Writing values to the file
    with open("outputmodel.txt", "w") as file:
        file.write("{}, {}, {}\n".format("user_id","bussiness_id","prediction"))
        for tup in predicted_values.collect():
            file.write("{}, {}, {}\n".format(inverted__user_dict[tup[0][0]],inverted__business_dict[tup[0][1]], float(tup[1])))


    print("RMSE" , RMSE)
    end = time.time()
    print("Time Taken", end-start)





