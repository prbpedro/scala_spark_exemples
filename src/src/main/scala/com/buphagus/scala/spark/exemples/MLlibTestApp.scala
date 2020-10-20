package com.buphagus.scala.spark.exemles

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object MLlibTestApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HiveTest")
      .set("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .set("hive.metastore.uris", "thrift://0.0.0.0:9083")
      .set("spark.sql.catalogImplementation", "hive")

    val sc: SparkContext = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.table("reduceduserdata").toDF()
    println(df.count() + " registros gravados na tabela ReducedUserData do hive")
    df.printSchema()
    df.show()
    println(df.head())

    showCorrelation(spark, df)
    executeLinearRegression(df)
  }

  def executeLinearRegression(df: org.apache.spark.sql.DataFrame) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("country_vec", "gender_vec", "age"))
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol("salary")
      .setFeaturesCol("features")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 1.0))
      .build()

    val steps: Array[org.apache.spark.ml.PipelineStage] = Array(assembler, lr)
    val pipeline = new Pipeline().setStages(steps)

    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol("salary"))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)

    val Array(training, test) = df.randomSplit(Array(0.75, 0.25), seed = 12345)

    val model = tvs.fit(training)

    val holdout = model.transform(test).select("prediction", "salary")

    val rm = new RegressionMetrics(holdout.rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
    println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")

    model.transform(test)
      .select("features", "salary", "prediction")
      .show(false)
  }

  def showCorrelation(spark: SparkSession, df: DataFrame) {
    
    import org.apache.spark.sql.functions._
    import spark.implicits._
    
    var corr = df.stat.corr("country_index", "salary")
    println("Correlation country X salary: " + corr)
    
    corr = df.stat.corr("age", "salary")
    println("Correlation age X salary: " + corr)
    
    corr = df.stat.corr("gender_index", "salary")
    println("Correlation gender_index X salary: " + corr)
  }
}