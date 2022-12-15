import pandas as pd
from pyspark.sql import SparkSession
import glob
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import col


spark = (SparkSession
        .builder
        .appName("Clustering model")
        .getOrCreate())

filename = glob.glob("preprocessed_data/*.csv")

df = (spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", "\t")
        .load(filename))
# df.show(n=5, truncate=False)
# print(df.count(), len(df.columns))

id_cols = ['code','product_name']
nutrition_cols = ['energy_100g',
                    'proteins_100g',
                    'fat_100g',
                    'carbohydrates_100g',
                    'sugars_100g',
                    'energy-kcal_100g',
                    'saturated-fat_100g',
                    'salt_100g',
                    'sodium_100g',
                    'fiber_100g',
                    'fruits-vegetables-nuts-estimate-from-ingredients_100g',
                    'nutrition-score-fr_100g']

df = df.dropna()
# print("SIZE DATA", dataset.count())
feat_df = df#[nutrition_cols]
# feat_df.printSchema()

feat_df = feat_df.select(*(col(c).cast("float").alias(c) for c in feat_df.columns))
# feat_df.printSchema()

assemble = VectorAssembler(inputCols=nutrition_cols, outputCol='features')
assembled_data = assemble.transform(feat_df)
# assembled_data.show(2)

scale = StandardScaler(inputCol='features',outputCol='standardized')
data_scale = scale.fit(assembled_data)
data_scale_output = data_scale.transform(assembled_data)
# data_scale_output.show(2)

# silhouette_score=[]
# evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
#                                 metricName='silhouette', distanceMeasure='squaredEuclidean')
# for i in range(3,8):
    
#     KMeans_algo=KMeans(featuresCol='standardized', k=i)
    
#     KMeans_fit=KMeans_algo.fit(data_scale_output)
    
#     output=KMeans_fit.transform(data_scale_output)
    
#     score=evaluator.evaluate(output)
    
#     silhouette_score.append(score)
    
#     print("Silhouette Score:", score)

# Trains a k-means model.
KMeans_algo = KMeans(featuresCol='standardized', k=7)
KMeans_fit = KMeans_algo.fit(data_scale_output)

# Make predictions
output = KMeans_fit.transform(data_scale_output)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

score=evaluator.evaluate(output)
print("Silhouette with squared euclidean distance = " + str(score))
# output.select(["prediction"]).show(n=5)

# Write CSV file with column header (column names)
result = output.select(id_cols + nutrition_cols + ["prediction"])
result.write.option("header",True).option("delimiter", "\t").csv("clustering_data")


