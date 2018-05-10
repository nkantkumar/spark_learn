package com.scb.cduls
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
// $example off:schema_inferring$
import org.apache.spark.sql.Row
// $example on:init_session$
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object SQLTest {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config("spark.some.config.option", "some-value").master("local[2]")
      .getOrCreate()

    import spark.implicits._
    // $example off:init_session$
    val df = spark.read.json("/Users/nishi/Documents/workspace/spark_learn/src/main/resources/people.json")
   // df.show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

   
  }
}