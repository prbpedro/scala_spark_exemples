package com.buphagus.scala.spark.exemles

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HiveSparkTestApp {

  def main(args: Array[String]) = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("ParquetTest")
      .set("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .set("hive.metastore.uris", "thrift://0.0.0.0:9083")
      .set("spark.sql.catalogImplementation", "hive")

    val sc: SparkContext = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val dabasesDfSql = spark.sql("show databases")
    dabasesDfSql.collect().foreach(println)

    spark.sql("use dbdesafio")

    val tablesDf = spark.sql("show tables")
    tablesDf.show

    val describeTableDf = spark.sql("describe dadoscovid")
    describeTableDf.show

    spark.table("dadoscovid").show

  }
}