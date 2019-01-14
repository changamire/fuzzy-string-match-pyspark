from pyspark.sql import SparkSession, Row
from pyspark.ml import Pipeline, Model, PipelineModel
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.ml.tuning import CrossValidator

from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col, lit, udf, concat, year, col


def test_matching_ml_pipeline(spark_context):
    match_names(get_name_frame_1(spark_context),
                get_name_frame_2(spark_context))


def match_names(df_1, df_2):

    pipeline = Pipeline(stages=[
        RegexTokenizer(
            pattern="", inputCol="name", outputCol="tokens", minTokenLength=1
        ),
        NGram(n=3, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors"),
        MinHashLSH(inputCol="vectors", outputCol="lsh")
    ])

    model = pipeline.fit(df_1)
  
    #model = PipelineModel.load("model")
    #print(model)
    #model.save('model')

    stored_hashed = model.transform(df_1)
    stored_hashed.select('name', 'vectors', 'lsh').show(20, False)
    landed_hashed = model.transform(df_2)
    landed_hashed.select('name',  'vectors', 'lsh').show(20, False)

    matched_df = model.stages[-1].approxSimilarityJoin(stored_hashed, landed_hashed, 1.0, "confidence").select(
        col("datasetA.name"), col("datasetB.name"), col("confidence"))
    matched_df.show(20, False)


def get_name_frame_2(spark_context):
    spark = SparkSession(spark_context)
    names = []
    names.append(get_row(1, 'John Smyth'))
    names.append(get_row(2, 'John Smith'))
    names.append(get_row(3, 'Jo Smith'))
    names.append(get_row(4, 'Bob Jones'))
    names.append(get_row(5, 'Tim Jones'))
    names.append(get_row(6, 'Laura Tully'))
    names.append(get_row(7, 'Sheena Easton'))
    names.append(get_row(8, 'Hilary Jones'))
    names.append(get_row(9, 'Hannah Short'))
    names.append(get_row(10, 'Greg Norman'))
    return spark.createDataFrame(names)


def get_name_frame_1(spark_context):
    spark = SparkSession(spark_context)
    names = []
    names.append(get_row(1, 'John Smith'))
    names.append(get_row(2, 'John Smith'))
    names.append(get_row(3, 'Jon Smithers'))
    names.append(get_row(4, 'Bob Jones'))
    names.append(get_row(5, 'Ned Flanders'))
    names.append(get_row(6, 'Lisa Short'))
    names.append(get_row(7, 'Joe Tan'))
    names.append(get_row(8, 'Jim Jones'))
    names.append(get_row(9, 'Chris Smith'))
    names.append(get_row(10, 'Norm Smith'))
    return spark.createDataFrame(names)


def get_row(id, name):
    return Row(
        id=id,
        name=name
    )