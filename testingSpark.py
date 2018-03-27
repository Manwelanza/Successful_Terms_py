from pyspark.ml.regression import LinearRegression
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors

from pyspark.sql.types import FloatType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import udf


def addtest (a,b):
    return a + b

def getFeatures (*attributes):
    """
    attributes parameters:
        * 0 --> Characters
        * 1 --> followers_count
        * 2 --> verified
        * 3 --> urls
        * 4 --> hahstags
        * 5 --> mentions
        * 6 --> coordinates
        * 7 --> isReply
        * 8 --> isQuote
        * 9 --> dateTime as String
    """
    features = []
    features.append(0.0 if int(attributes[0]) <= 0 else float(attributes[0]))
    features.append(float (attributes[1]))


    return features

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

"""
#df.withColumn("characters", 0 if df.characters <= 0 else df.characters)
df.withColumn("followers", int (df.user.followers_count))
df.withColumn("verified", 1 if df.user.verified else 0)
df.withColumn("media", 1 if df.urls != None and len(df.urls) > 0 else 0 )
df.withColumn("hashtags", 1 if df.hashtags != None and len(df.hashtags) > 0 else 0 )
df.withColumn("mentions", 1 if df.mentions != None and len(df.mentions) > 0 else 0 )
df.withColumn("coordinates", 1 if df.coordinates != None and df.coordinates else 0)
df.withColumn("rp", 1 if df.isReply else 0)
df.withColumn("qt", 1 if df.isQuote else 0)
df.withColumn("weekDay", df.created_at.split()[0])
#df.withColumn("yearDay", )
df.withColumn("time", int(df.created_at.split()[3].split(":")[0]) * 60 + int(df.created_at.split()[3].split(":")[1]))

df.drop("_id", "RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
"tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
"quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
"visibility_value_normalized", "RS_normalized", "PD_normalized")


df.show(1)
"""
"""
func_udf = udf(addtest, FloatType())
df = df.withColumn("a+b", func_udf(df.a, df.b))

df.show()
"""

getFeatures_udf = udf (getFeatures, ArrayType(FloatType()))
df = df.withColumn("features", getFeatures_udf(df.characters, df.user.followers_count, df.user.verified, df.urls, \
                                                df.hashtags, df.mentions, df.coordinates, df.isReply, df.isQuote, \
                                                df.created_at)) \
        .withColumnRenamed("AVG_M", "label") \
        .drop("_id", "RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                "visibility_value_normalized", "RS_normalized", "PD_normalized", "coordinates", "media", "terms_count", "mentions", \
                "hashtags", "characters") \

df.show(5)

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