package com.scb.cduls
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object HdfsReadWrite {
     def main(args : Array[String]){
    val conf = new SparkConf().setAppName("HDFS Read Write").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile("hdfs://localhost:9000/nishi/cars.csv")
    val wordspair =textFile.flatMap(row =>row.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wordspair.foreach(println)
    //save as flat file
    wordspair.saveAsTextFile("hdfs://localhost:9000/nishi/processed_cars.csv")
    
    // val df = spark..read.option("header","true").csv("csv_file.csv")
     //df.write.parquet("csv_to_paraquet")
    // val df_1 = spark.read.option("header","true").parquet("csv_to_paraquet")
  }
  
  
}