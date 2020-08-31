package com.buphagus.scala.spark.exemles

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.Row

/**
 * @author Pedro Ribeiro Baptista
 */
object ParquetTestApp {

  def main(args: Array[String]) = {
    // Two threads local[2]
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    readParquet(sqlContext)
  }

  def readParquet(sqlContext: SQLContext) = {
    val userDataDF = sqlContext.read.parquet("hdfs://localhost:9000/input/spark/parquettest/userdata1.parquet")

    userDataDF.show(true);

    import sqlContext.implicits._
    val userDataFilteredDF = userDataDF.filter($"email" !== "")
    val userDataRdd = userDataFilteredDF.rdd

    val mapLastName = userDataRdd.map(row => {

      (row.getAs[String]("last_name"), row.getAs[String]("first_name"))
    })
    mapLastName.collect().foreach(println)

    val reduceLastNameCount = mapLastName.reduceByKey((x, y) => x + y)
    reduceLastNameCount.collect().foreach(println)

    
  }
}