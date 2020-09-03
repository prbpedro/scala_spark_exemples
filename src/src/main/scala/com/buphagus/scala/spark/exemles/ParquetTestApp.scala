package com.buphagus.scala.spark.exemples

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder

/**
 * spark-submit --class com.buphagus.scala.spark.exemples.ParquetTestApp /home/prbpedro/Development/repositories/github/scala_spark_exemples/src/target/scala.spark.exemples-0.0.1-SNAPSHOT.jar
 * 
 * @author Pedro Ribeiro Baptista
 */
object ParquetTestApp {

  def main(args: Array[String]) : Unit = {
     val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("ParquetTest")
      .set("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .set("hive.metastore.uris", "thrift://0.0.0.0:9083")
      .set("spark.sql.catalogImplementation", "hive")

    val sc: SparkContext = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark (Parquet -> Hive) Example")
      .getOrCreate()

    var df = readParquet(spark)
    df = preProcess(spark, df)
    df.write.option("path", "/output/sparkparquethive").mode(SaveMode.Overwrite).saveAsTable("ReducedUserData")
  }

  def readParquet(spark: SparkSession): DataFrame = {
    return spark.read.parquet("hdfs://localhost:9000/input/spark/parquettest/userdata1.parquet")
  }
  
  def preProcess(spark: SparkSession, df: DataFrame): DataFrame = {
    var reducedDf = df.select("id", "gender", "salary", "country")
    reducedDf.printSchema()
    
    println("------------- Null Values Count -----------")
    getNullValuesCount(reducedDf).foreach(p => println("(" + p._1 + ", " + p._2 + ")"))
    println("-------------------------------------------\n")
    
    reducedDf = transformNullGenderValues(reducedDf)
    reducedDf = transformNullSalaryValues(spark, reducedDf)
        
    println("--- Null Values Count after transform -----")
    getNullValuesCount(reducedDf).foreach(p => println("(" + p._1 + ", " + p._2 + ")"))
    println("-------------------------------------------\n")
    
    println("------------- Outliers detection ----------")
    findSalaryOutliers(reducedDf)
    println("-------------------------------------------\n")
    
    println("--------------- Label Encoding ------------")
    reducedDf = applyEnconding(spark, reducedDf)
    println("-------------------------------------------\n")

    return reducedDf
  }
  
  def applyEnconding(spark: SparkSession, df: DataFrame): DataFrame = {
    var reducedDf = new StringIndexer().setInputCol("gender").setOutputCol("gender_index").fit(df).transform(df);
    reducedDf = new OneHotEncoder().setInputCol("gender_index").setOutputCol("gender_vec").fit(reducedDf).transform(reducedDf);
    
    reducedDf = new StringIndexer().setInputCol("country").setOutputCol("country_index").fit(reducedDf).transform(reducedDf);
    reducedDf = new OneHotEncoder().setInputCol("country_index").setOutputCol("country_vec").fit(reducedDf).transform(reducedDf);
    
    reducedDf.show()
    reducedDf.printSchema()
    return reducedDf
  }

  def findSalaryOutliers(reducedDf: DataFrame) = {
    val quantiles = reducedDf.stat.approxQuantile("salary", Array(0.05, 0.95),0.0)
    val lowerRange = quantiles(0)
    val upperRange = quantiles(1)
    
    reducedDf.filter(reducedDf("salary").lt(lowerRange)).select("salary").describe().show()
    reducedDf.filter(reducedDf("salary").gt(upperRange)).select("salary").describe().show()
  }
  
  def transformNullSalaryValues(spark: SparkSession, reducedDf: DataFrame) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    
    val dDf = reducedDf.select("salary").describe()
    dDf.printSchema()
    val salMean = dDf.where(dDf("summary") === "mean").first().getAs[String]("salary").toDouble
    reducedDf.na.fill(salMean,Array("salary"))
  }

  def transformNullGenderValues(reducedDf: DataFrame) = {
    import org.apache.spark.sql.functions._
    
    val genderNullTransformation = udf {(id: Int, gender: String) => 
      if(gender == "" || gender == null){
         if(id%2 == 0) "Male" else "Female"
      } else gender
    }
    
    reducedDf.withColumn("gender", genderNullTransformation(reducedDf("id") , reducedDf("gender")))
  }

  def getNullValuesCount(df: DataFrame):  Map[String, Long] = {
    val cache = Map[String, Long]()
    
    df.columns.foreach(col => {
      val countDf = df.where(df(col).isNull || df(col) === "" || df(col).isNaN)
      val countNullCol = countDf.count()
      cache += (col -> countNullCol)
    })
    
    return cache
  }
  
  def executeMapReduce(userDataRdd: RDD[Row]) = {
    val mapLastName = userDataRdd.map(row => {

      (row.getAs[String]("last_name"), row.getAs[String]("first_name"))
    })
    mapLastName.collect().foreach(println)

    val reduceLastNameCount = mapLastName.reduceByKey((x, y) => x + y)
    reduceLastNameCount.collect().foreach(println)
  }
}