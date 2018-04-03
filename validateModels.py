from pyspark.ml.evaluation import RegressionEvaluator

class ValidateModels ():
    MSE = "mse" #Mean Squared Error
    RMSE = "rmse" #Root Mean Squared Error
    MAE = "mae" #Mean absolute error

    LR = "lr"
    GLR = "glr"
    RFR = "rfr"

    def __init__ (self):
        self.metrics = {
            ValidateModels.LR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None
            },
            ValidateModels.GLR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None
            },
            ValidateModels.RFR : {
                ValidateModels.MSE : None,
                ValidateModels.RMSE : None,
                ValidateModels.MAE : None
            }
        }
    
    def validate (self, df, model, modelName=None):
        predictions0 = model.transform(df)

        evaluator = RegressionEvaluator(metricName=ValidateModels.MSE)
        mse = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.RMSE)
        rmse = evaluator.evaluate(predictions0)
        evaluator.setMetricName(ValidateModels.MAE)
        mae = evaluator.evaluate(predictions0)

        if modelName != None:
            self.metrics[modelName][ValidateModels.MSE] = mse
            self.metrics[modelName][ValidateModels.RMSE] = rmse
            self.metrics[modelName][ValidateModels.MAE] = mae
        
        return {
            ValidateModels.MSE : mse,
            ValidateModels.RMSE : rmse,
            ValidateModels.MAE : mae
        }

    
    def getMetrics (self, modelName=None, metricName=None):
        if metricName != None and modelName != None:
            return self.metrics[modelName][metricName]
        
        elif metricName == None and modelName != None:
            return self.metrics[modelName]

        else:
            return self.metrics

    def isValidated (self, modelName):
        return self.metrics[modelName][ValidateModels.MSE] != None

