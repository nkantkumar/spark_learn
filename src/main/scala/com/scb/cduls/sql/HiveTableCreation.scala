package com.scb.cduls.sql
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object HiveTableCreation {
   def main(args: Array[String]) {
  
    val spark = SparkSession.builder().appName("read-and-write-from-hdfs").master("local")             
                      .enableHiveSupport()
                      .getOrCreate()
                      
                      
  import spark.implicits._
  import spark.sql
  sql("CREATE DATABASE IF NOT EXISTS finance")
  sql("use finance")
  sql("CREATE TABLE src(id int) ")
  
   }
}