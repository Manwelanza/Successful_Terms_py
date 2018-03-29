from pyspark.ml.regression import LinearRegression
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import udf
from tools4Spark import *

#sc = SparkContext('local')
#spark = SparkSession(sc)
#spark = SparkSession.builder.master("local").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("myAppTesting") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test1.clearTweet") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test1.clearTweet") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/test1.clearTweet").load()

getFeatures_udf = udf (getFeatures, ArrayType(IntegerType()))
getCharacters_udf = udf (getCharacters, IntegerType())
getFollowers_udf = udf (getFollowers, IntegerType())
getVerified_udf = udf (getVerified, IntegerType())
getMedia_udf = udf (getMedia, IntegerType())
getHashtags_udf = udf (getHashtags, IntegerType())
getMentions_udf = udf (getMentions, IntegerType())
getCoordinates_udf = udf (getCoordinates, IntegerType())
getRP_udf = udf (getRP, IntegerType())
getQT_udf = udf (getQT, IntegerType())
getTimeCol_udf = udf (getTimeCol, IntegerType())
#df = df.withColumn("coordinates", df.coordinates.cast("string"))

df = df.withColumn("characters", getCharacters_udf(df.characters))
df = df.withColumn("followers", getFollowers_udf(df.user.followers_count))
df = df.withColumn("verified",getVerified_udf(df.user.verified))
df = df.withColumn("media", getMedia_udf(df.urls))
df = df.withColumn("hashtags", getHashtags_udf(df.hashtags))
df = df.withColumn("mentions", getMentions_udf(df.mentions))
#df = df.withColumn("coordinates", getCoordinates_udf(df.coordinates))
df = df.withColumn("rp", getRP_udf(df.isReply))
df = df.withColumn("qt", getQT_udf(df.isQuote))
#df.withColumn("weekDay", df.created_at.split()[0])
#df.withColumn("yearDay", )
df = df.withColumn("time", getTimeCol_udf(df.created_at))
df = df.withColumnRenamed("AVG_M", "label")
df = df.drop("RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                "visibility_value_normalized", "RS_normalized", "PD_normalized", "terms_count", "coordinates")

"""
df = df.withColumn("features", getFeatures_udf(df.characters, df.user.followers_count, df.user.verified, df.urls, \
                                                df.hashtags, df.mentions, df.coordinates, df.isReply, df.isQuote, \
                                                df.created_at)) \
        .withColumnRenamed("AVG_M", "label") \
        .drop("_id", "RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                "visibility_value_normalized", "RS_normalized", "PD_normalized", "coordinates", "media", "terms_count", "mentions", \
                "hashtags", "characters")
"""

assembler = VectorAssembler(
  inputCols=["characters", "followers", "verified", "media", "hashtags", "mentions", "rp", "qt", "time"], outputCol="features"
)
df = assembler.transform(df)
#df.count()
#df.show(5)

splitDF = df.randomSplit([0.2, 0.2, 0.6])

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
# Fit the model
lrModel = lr.fit(splitDF[2])

predictions0 = lrModel.transform(splitDF[1])
evaluator = RegressionEvaluator(metricName="mse")
mse = evaluator.evaluate(predictions0)
evaluator.setMetricName("rmse")
rmse = evaluator.evaluate(predictions0)
evaluator.setMetricName("mae")
mae = evaluator.evaluate(predictions0)
print("Mean Squared Error: " + str(mse))
print("Root Mean Squared Error: " + str(rmse))
print("Mean absolute error: " + str(mae))



# Load training data
"""training = spark.read.format("libsvm")\
    .load("sparkData/sample_regression_data.txt")"""

"""

training = spark.createDataFrame(
    [
        (0, Vectors.dense([1])),
        (0, Vectors.dense([2])),
        (1, Vectors.dense([3])),
        (1, Vectors.dense([4]))
    ],
    ["label", "features"]
)

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(training)

test0 = spark.createDataFrame(
    [
        (0, Vectors.dense([-1])),
        (0, Vectors.dense([1])),
        (1, Vectors.dense([6])),
        (1, Vectors.dense([4]))
    ],
    ["label", "features"]
)
predictions0 = lrModel.transform(test0)
print(predictions0.collect)
result = predictions0.select("features", "label", "prediction") \
    .collect()

for row in result:
    print("features=%s, label=%s -> prediction=%s"
          % (row.features, row.label, row.prediction))

"""
"""
# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)"
"""