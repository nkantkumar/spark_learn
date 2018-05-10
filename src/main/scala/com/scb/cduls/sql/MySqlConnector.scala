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
import java.util.Properties
object MySqlConnector {
  def main(args: Array[String]) {
     
    val spark = SparkSession.builder().appName("read-and-write-from-mysql").master("local").getOrCreate()
    
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306")
         .option("dbtable", "my_db.employees")
         .option("user", "root")
         .option("password", "")
         .load()
   
      val connectionProperties = new Properties()
      connectionProperties.put("user", "root")
      connectionProperties.put("password", "")    

      val employees_df = spark.read.jdbc("jdbc:mysql://localhost:3306", "my_db.employees", connectionProperties)
      employees_df.createOrReplaceTempView("emp")
       import spark.implicits._
    // employees_df.show()
     employees_df.select("last_name").filter($"department_id" > 90).show()
      //employees_df.select($"last_name", $"email" ).show()
      
  }
}