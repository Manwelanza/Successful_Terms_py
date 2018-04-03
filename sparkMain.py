from SparkTool import SparkTool
from modelsSpark import ModelsSpark



if __name__ == "__main__":
    sparkTool = SparkTool("Huelga", "clearTweet")
    modelSpark = ModelsSpark(sparkTool)

    lrModel = modelSpark.getOrCreateLR()
    glrModel = modelSpark.getOrCreateGLR()
    rfrModel = modelSpark.getOrCreateRFR()

    lr = modelSpark.predictionAndValidation(sparkTool.getFullDf(), lrModel)
    glr = modelSpark.predictionAndValidation(sparkTool.getFullDf(), glrModel)
    rfr = modelSpark.predictionAndValidation(sparkTool.getFullDf(), rfrModel)

    print (lr[1])
    print (glr[1])
    print (rfr[1])

    for row in rfr[0]:
        if (row.label > 0.1):
            print("label=%s -> prediction=%s"
                % (row.label, row.prediction))

