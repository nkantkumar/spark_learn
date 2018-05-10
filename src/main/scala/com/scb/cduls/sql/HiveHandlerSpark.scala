package com.scb.cduls.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._


import org.apache.spark.sql.{SQLContext, Row, DataFrame}
object HiveHandlerSpark {
   case class YahooStockPrice(date: String, open: Float, high: Float, low: Float, close: Float, volume: Integer, adjClose: Float)
  def main(args: Array[String]) {
     
  val conf = new SparkConf().setAppName("Test").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(conf)
 
 val sqlContext = new SQLContext(sc)
  
import sqlContext.implicits._
 // sqlContext.load("hdfs://localhost:9000/tmp/yahoo_stocks.csv").registerTempTable("yahoo_stock_tmp")
  val yahoo_stocks = sc.textFile("hdfs://localhost:9000/yahoo_stocks.csv")
  val data = yahoo_stocks.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  val stockprice = data.map(_.split(",")).map(row => YahooStockPrice(row(0), row(1).trim.toFloat, row(2).trim.toFloat, row(3).trim.toFloat, row(4).trim.toFloat, row(5).trim.toInt, row(6).trim.toFloat))
  sqlContext.createDataFrame(stockprice).registerTempTable("yahoo_stocks_temp")
  val results = sqlContext.sql("SELECT * FROM yahoo_stocks_temp where low > 45")
  //sqlContext.sql("create table yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")
  
  //results.show()
  //results.write.mode(SaveMode.Overwrite).format("orc").save("hdfs://localhost:9000/tmp/hive/warehouse/yahoo_stocks_orc")
  val test_enc_orc = sqlContext.table("finance.yahoo_stocks_txt_table")
  
  test_enc_orc.printSchema()
 
   }
  
}