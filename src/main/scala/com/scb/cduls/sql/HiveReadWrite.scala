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
import java.io.File

object HiveReadWrite {
 case class Record(key: Int, value: String)
 val warehouseLocation = new File("saprk-warehouse").getAbsolutePath
  def main(args: Array[String]) {
  
    val spark = SparkSession.builder().appName("read-and-write-from-hdfs").master("local")
                      .config("spark.sql.warehouse.dir", warehouseLocation)
                      .enableHiveSupport()
                      .getOrCreate()
  import spark.implicits._
  import spark.sql
  sql("CREATE DATABASE IF NOT EXISTS finance")
  sql("use finance")
  sql("DROP TABLE IF EXISTS employee")
   sql("CREATE TABLE IF NOT EXISTS "
         +" employee (name String, "
         +" salary String)"
         +" ROW FORMAT DELIMITED"
         +" FIELDS TERMINATED BY ','"
         +" STORED AS TEXTFILE")
  sql("LOAD DATA LOCAL INPATH '/Users/nishi/Documents/workspace/spark_learn/src/main/resources/util/employee.txt' INTO TABLE employee")
  sql("ALTER TABLE employee ADD PARTITION (salry=’1000’)")
  sql("SELECT * FROM employee where salary > '1000' order by name").show()
  val recordsDF =  sql("SELECT * FROM employee where salary > '1000'")

  recordsDF.createOrReplaceTempView("records")
  sql("SELECT * FROM records r left outer JOIN employee s ON r.name = s.name").show()
  }
}