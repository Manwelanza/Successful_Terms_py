from crossValidation import bestLinearReggresion, bestGeneralizedLR, bestRandomForestRegressor
from pyspark.ml.regression import LinearRegressionModel, GeneralizedLinearRegressionModel, RandomForestRegressionModel
from pyspark.ml.regression import LinearRegression

from validateModels import ValidateModels
from SparkTool import SparkTool

CONST_LR_FILE = "file:///D:/Data_TFM/code/models/LinearRegression2"
CONST_GLR_FILE = "file:///D:/Data_TFM/code/models/GeneralizedLinearRegression2"
CONST_RFR_FILE = "file:///D:/Data_TFM/code/models/RandomForestRegressor2" 


class ModelsSpark():

    def __init__ (self, sparkTool):
        self.sparkTool = sparkTool
        self.lrModel = None
        self.glrModel = None
        self.rfrModel = None
        self.validations = ValidateModels ()


    def getOrCreateLR (self):
        try:
            if self.lrModel == None:
                self.lrModel = LinearRegressionModel.load(CONST_LR_FILE)
        except :
            print("Creating LR Model")
            self.lrModel =  self.createLR ()
        
        return self.lrModel

    def createLR (self):
        try:
            lrModel = bestLinearReggresion (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), lrModel, ValidateModels.LR)
        except:
            print("RFR = None")
            lrModel = None

        try:
            lrModel.write().overwrite().save(CONST_LR_FILE)
        except :
            print("saved LR MODEL")
        
        return lrModel

    def getOrCreateGLR (self):
        try:
            if self.glrModel == None:
                self.glrModel = GeneralizedLinearRegressionModel.load(CONST_GLR_FILE)
        except :
            print("Creating GLR Model")
            self.glrModel = self.createGLR ()

        return self.glrModel

    def createGLR (self):
        try:
            glrModel = bestGeneralizedLR (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), glrModel, ValidateModels.GLR)
        except:
            print("GLR = None")
            glrModel = None
        
        try:
            glrModel.write().overwrite().save(CONST_GLR_FILE)
        except:
            print("Error saving GLR MODEL")

        return glrModel

    def getOrCreateRFR (self):
        try:
            if self.rfrModel == None:
                self.rfrModel = RandomForestRegressionModel.load(CONST_RFR_FILE)
        except :
            print("Creating RFR Model")
            self.rfrModel = self.createRFR ()

        return self.rfrModel

    def createRFR (self):
        try:
            rfrModel = bestRandomForestRegressor (self.sparkTool.getTrainDF(), self.sparkTool.getMetricDF(), "mse")
            self.validations.validate(self.sparkTool.getTestDF(), rfrModel, ValidateModels.RFR)

        except :
            print("RFR = None")
            rfrModel = None
        
        try:
            rfrModel.write().overwrite().save(CONST_RFR_FILE)
        except :
            print("Error saving RFR MODEL")
        
        return rfrModel

    def lrValidated (self):
        return self.validations.isValidated(ValidateModels.LR)
    
    def glrValidated (self):
        return self.validations.isValidated(ValidateModels.GLR)

    def rfrValidated (self):
        return self.validations.isValidated(ValidateModels.RFR)

    def getBestModel (self, metricname=ValidateModels.MSE):
        if not self.lrValidated:
            self.getOrCreateLR()
            self.validations.validate(self.sparkTool.getTestDF(), self.lrModel, ValidateModels.LR)

        if not self.glrValidated:
            self.getOrCreateGLR()
            self.validations.validate(self.sparkTool.getTestDF(), self.glrModel, ValidateModels.GLR)

        if not self.rfrValidated:
            self.getOrCreateRFR()
            self.validations.validate(self.sparkTool.getTestDF(), self.rfrModel, ValidateModels.RFR)

        bestModel = self.lrModel
        bestMetric = self.validations.getMetrics(ValidateModels.LR, metricname)

        if bestMetric > self.validations.getMetrics(ValidateModels.GLR, metricname):
            bestMetric = self.validations.getMetrics(ValidateModels.GLR, metricname)
            bestModel = self.glrModel

        if bestMetric > self.validations.getMetrics(ValidateModels.RFR, metricname):
            bestMetric = self.validations.getMetrics(ValidateModels.RFR, metricname)
            bestModel = self.rfrModel

        return bestModel

   
    def prediction (self, dataDF=None, model=None):
        if dataDF == None:
           return None

        if model == None:
            model = self.getBestModel()

        predictions = model.transform(dataDF)
        return predictions.select("features", "label", "prediction").collect()


    def predictionAndValidation (self, dataDF=None, model=None):
        if dataDF == None:
           return None

        if model == None:
            model = self.getBestModel()

        predictions = model.transform(dataDF)
        return [predictions.select("features", "label", "prediction").collect(), self.validations.validate(dataDF, model, None)]