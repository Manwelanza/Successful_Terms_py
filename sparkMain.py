from SparkTool import SparkTool
from modelsSpark import ModelsSpark



if __name__ == "__main__":
    modelType = SparkTool.CONST_TYPE_CLASIFICATION
    sparkTool = SparkTool(modelType, "test1", "clearTweet")
    modelSpark = ModelsSpark(sparkTool)

    
    if modelType == SparkTool.CONST_TYPE_CLASIFICATION:
        model1 = modelSpark.getOrCreateLRC()
        #model2 =
        #model3 =  
    else:
        model1 = modelSpark.getOrCreateLR()
        model2 = modelSpark.getOrCreateGLR()
        model3 = modelSpark.getOrCreateRFR()
        

    result1 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model1)
    #result2 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model2)
    #result3 = modelSpark.predictionAndValidation(modelType == SparkTool.CONST_TYPE_REGRESSION, sparkTool.getTestDF(), model3)
    

    print (result1[1])
    #print (result2[1])
    #print (result3[1])

    for row in result1[0]:
        if (modelType == SparkTool.CONST_TYPE_REGRESSION and row.label > 0.1):
            print("label=%s -> LR-prediction=%s"
                % (row.label, row.prediction))

    """for row in result2[0]:
        if (row.label > 0.1):
            print("label=%s -> GLR-prediction=%s"
                % (row.label, row.prediction))

    
    for row in result3[0]:
        if (row.label > 0.1):
            print("label=%s -> RFR-prediction=%s"
                % (row.label, row.prediction))"""

    