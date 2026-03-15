#put instruction to run your code at home directory here and in google form

#download dataset set at https://github.com/cchantra/bigdata.github.io/raw/refs/heads/master/spark/animedataset.zip

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('anime').getOrCreate()


print('******<your name1, your name 2> ******')

lines = spark.read.csv("hdfs://localhost:9000/anime/ratings.csv",header = True, inferSchema = True).rdd

ratingsRDD = .....
ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.8, 0.2])


als = ALS(maxIter=5, regParam=0.01,  .......
          coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# Generate top 5 anime recommendations for each user
userRecs = model.recommendForAllUsers(5)
# Generate top 5 user recommendations for each anime
animeRecs = model.recommendForAllItems(10)


predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")


selected_userid = [20568,9524,3196,1277,54]

# Generate top 5  anime recommendations for a specified set of users
selected_userid = [20568,9524,3196,1277,54]
userSubsetRecs = model.recommendForUserSubset(selected_user_id, 5)

#print animate name  userid : anime1, anime2, ....


........

print('**** Done ****')
