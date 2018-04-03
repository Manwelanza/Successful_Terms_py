from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from tools4Spark import *

class SparkTool ():

    def __init__ (self, dbName="test1", collectionName="clearTweet"):
        self.db = dbName
        self.collection = collectionName
        self.splitDF = None

        self.initSpark()
        self.createDBConnection()
        self.initUdfFunction()
        self.transformDF()

    def initSpark (self):
        self.spark = SparkSession \
        .builder \
        .appName("myAppTesting") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/" + self.db + "." + self.collection) \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + self.db + "." + self.collection) \
        .getOrCreate()

    def createDBConnection (self):
        self.df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
            "mongodb://127.0.0.1/" + self.db + "." + self.collection).load()

    def initUdfFunction (self):
        self.getCharacters_udf = udf (getCharacters, IntegerType())
        self.getFollowers_udf = udf (getFollowers, IntegerType())
        self.getVerified_udf = udf (getVerified, IntegerType())
        self.getMedia_udf = udf (getMedia, IntegerType())
        self.getHashtags_udf = udf (getHashtags, IntegerType())
        self.getMentions_udf = udf (getMentions, IntegerType())
        self.getCoordinates_udf = udf (getCoordinates, IntegerType())
        self.getRP_udf = udf (getRP, IntegerType())
        self.getQT_udf = udf (getQT, IntegerType())
        self.getTimeCol_udf = udf (getTimeCol, IntegerType())

    def transformDF (self):
        self.df = self.df.withColumn("characters", self.getCharacters_udf(self.df.characters))
        self.df = self.df.withColumn("verified", self.getVerified_udf(self.df.user.verified))
        self.df = self.df.withColumn("followers", self.getFollowers_udf(self.df.user.followers_count))
        self.df = self.df.withColumn("media", self.getMedia_udf(self.df.urls))
        self.df = self.df.withColumn("hashtags", self.getHashtags_udf(self.df.hashtags))
        self.df = self.df.withColumn("mentions", self.getMentions_udf(self.df.mentions))
        #self.df = self.df.withColumn("coordinates", getCoordinates_udf(df.coordinates))
        self.df = self.df.withColumn("rp", self.getRP_udf(self.df.isReply))
        self.df = self.df.withColumn("qt", self.getQT_udf(self.df.isQuote))
        #self.df.withColumn("weekDay", self.df.created_at.split()[0])
        #self.df.withColumn("yearDay", )
        self.df = self.df.withColumn("time", self.getTimeCol_udf(self.df.created_at))
        self.df = self.df.withColumnRenamed("AVG_M", "label")
        self.df = self.df.drop("RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                        "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                        "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                        "visibility_value_normalized", "RS_normalized", "PD_normalized", "terms_count", "coordinates")
        
        assembler = VectorAssembler(
            inputCols=["characters", "followers", "verified", "media", "hashtags", "mentions", "rp", "qt", "time"], outputCol="features"
        )
        self.df = assembler.transform(self.df)

    def getSpark (self):
        return self.spark

    def getFullDf (self):
        return self.df

    def getTrainDF (self):
        if self.splitDF == None:
            self.splitDF = self.df.randomSplit([0.2, 0.2, 0.6])

        return self.splitDF[2]

    def getMetricDF (self):
        if self.splitDF == None:
            self.splitDF = self.df.randomSplit([0.2, 0.2, 0.6])

        return self.splitDF[1]

    def getTestDf (self):
        if self.splitDF == None:
            self.splitDF = self.df.randomSplit([0.2, 0.2, 0.6])

        return self.splitDF[0]