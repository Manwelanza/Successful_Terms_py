from SparkTool import SparkTool
from modelsSpark import ModelsSpark



if __name__ == "__main__":
    sparkTool = SparkTool("test1", "clearTweet")
    modelSpark = ModelsSpark(sparkTool)

    lrModel = modelSpark.getOrCreateLR()
    glrModel = modelSpark.getOrCreateGLR()
    rfrModel = modelSpark.getOrCreateRFR()

    lr = modelSpark.predictionAndValidation(sparkTool.getTestDF(), lrModel)
    glr = modelSpark.predictionAndValidation(sparkTool.getTestDF(), glrModel)
    rfr = modelSpark.predictionAndValidation(sparkTool.getTestDF(), rfrModel)

    print (lr[1])
    print (glr[1])
    print (rfr[1])

    for row in lr[0]:
        if (row.label > 0.1):
            print("label=%s -> LR-prediction=%s"
                % (row.label, row.prediction))

    for row in glr[0]:
        if (row.label > 0.1):
            print("label=%s -> GLR-prediction=%s"
                % (row.label, row.prediction))

    
    for row in rfr[0]:
        if (row.label > 0.1):
            print("label=%s -> RFR-prediction=%s"
                % (row.label, row.prediction))

