from pyspark.ml.regression import LinearRegression
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors

from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf


def addtest (a,b):
    return a + b

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
"mongodb://127.0.0.1/testingMongo.test").load()

#df.withColumn("a+b", df.a + df.b)
func_udf = udf(addtest, FloatType())
df = df.withColumn("a+b", func_udf(df.a, df.b))

#df2 = df.sample(True, .5)
#df2.drop("a+b")

df.show()
#df2.show()





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