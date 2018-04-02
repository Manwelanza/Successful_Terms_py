from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def getBestModel (models, data, metricToCompare):
    bestModel = None
    bestMetric = None
    evaluator = RegressionEvaluator(metricName=metricToCompare)

    for model in models:
        predictions = model.transform(data)
        metric = evaluator.evaluate(predictions)

        if bestMetric == None or bestMetric > metric:
            bestMetric = metric
            bestModel = model
    
    return bestModel

def bestLinearReggresion (trainDf, metricDF, metricToCompare):
    elasticNetParam = [1.0, 0.6, 0.2]
    regParam = [1.0, 0.6, 0.2]
    tol = [1.0, 0.6, 0.2, 0.0]
    models = []
    
    for e in elasticNetParam:
        for r in regParam:
            for t in tol:
                models.append(LinearRegression(maxIter=10, regParam=r, elasticNetParam=e, tol=t).fit(trainDf))
    
    return getBestModel(models, metricDF, metricToCompare)

def bestGeneralizedLR (trainDf, metricDF, metricToCompare):
    regParam = [1.0, 0.6, 0.2]
    tol = [1.0, 0.6, 0.2, 0.0]
    family = ["poisson", "gaussian"]
    link = {"poisson": ["identity", "sqrt", "log"], "gaussian": ["identity"]}
    models = []

    for r in regParam:
        for f in family:
            for l in link.get(f):
                for t in tol:
                    models.append(GeneralizedLinearRegression(maxIter=10, regParam=r, family=f, link=l, tol=t).fit(trainDf))

    return getBestModel(models, metricDF, metricToCompare)

def bestRandomForestRegressor (trainDf, metricDF, metricToCompare):
    numTrees = [40, 20, 10, 5]
    maxDepth = [10, 6, 2, 0]
    subsamplingRate = [1.0, 0.66, 0.33]
    models = []

    for nt in numTrees:
        for md in maxDepth:
            for ss in subsamplingRate:
                models.append(RandomForestRegressor(numTrees=nt, maxDepth=md, subsamplingRate=ss).fit(trainDf))

    return getBestModel(models, metricDF, metricToCompare)