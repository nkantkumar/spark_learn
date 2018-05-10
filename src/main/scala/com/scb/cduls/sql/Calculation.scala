package com.scb.cduls.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Calculation {
  case class Trans(accNo: String, tranAmount: Double)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Data Soures Example").master("local")
      .config("spark.some.config.option", "some-value")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    def toTrans = (trans: Seq[String]) => Trans(trans(0), trans(1).trim.toDouble)
    val acTransList = Array("SB10001,1000", "SB10002,1200",
      "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000",
      "SB10007,500", "SB10008,56", "SB10009,30", "SB10010,7000", "CR10001,7000",
      "SB10002,-10")

    val acTransRDD = sc.parallelize(acTransList).map(_.split(",")).map(toTrans(_))
    val acTransDF = spark.createDataFrame(acTransRDD)
    acTransDF.createOrReplaceTempView("trans")
    acTransDF.printSchema    
    acTransDF.show
    
    val highValueTransRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE tranAmount > 1000")
    highValueTransRecords.show
    
    val badAccountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE accNo NOT like 'SB%'")
    badAccountRecords.show
    
    val badAmountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE tranAmount < 0")
    
    val badTransRecords = badAccountRecords.union(badAmountRecords)
    val maxAmount = spark.sql("SELECT max(tranAmount) as max FROM trans")
    val minAmount = spark.sql("SELECT min(tranAmount) as min FROM strans")
  }
}