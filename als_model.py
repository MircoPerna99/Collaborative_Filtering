from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

class ALSModel():
    def __init__(self, data):
        self.data = data
    
    def train(self):
        (training, test) = self.data.randomSplit([0.8, 0.2])
        
        regParams = [0.01, 0.1]
        ranks = [25]
        alphas = [10.0, 20.0, 40.0, 60.0, 80.0]
        
        aus_regParam = 0.0
        aus_rank = 0
        aus_alpha = 0.0
        aus_rmse = 0.0
        
        for regParam in regParams:
            for rank in ranks:
                for alpha in alphas:
                    aus_als = ALS(maxIter = 10, regParam = regParam, rank = rank, alpha = alpha, userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "Interactions",coldStartStrategy = "drop")
                    
                    aus_model = aus_als.fit(training)
                    predictions = aus_model.transform(test)
                    evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "Interactions", predictionCol = "prediction")
                    rmse = evaluator.evaluate(predictions)
                    
                    if(aus_rmse == 0.0 or rmse < aus_rmse):
                        aus_regParam = regParam
                        aus_rank = rank
                        aus_alpha = alpha
                        aus_rmse = rmse

                    print("For regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(regParam, rank, alpha, rmse))
                     
        print("Chosen parameters: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(aus_regParam, aus_rank, aus_alpha, aus_rmse))        
                    
                