package com.scb.cduls
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

object RddTest {
   def main(args: Array[String]) {
       val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
       val sc = new SparkContext(conf)
       val stringRdd= sc.parallelize(Array("java","scala","java csript","c plus"))
       val filteredRdd = stringRdd.filter { x => x.startsWith("j") }
       filteredRdd.foreach { println }
       
       val valPairRdd = stringRdd.map { x => (x,1) }
       val wordCount = valPairRdd.reduceByKey((x,y)=>x+y)
       wordCount.foreach { println }
       
       val intRdd= sc.parallelize(Array(1,3,5,6,8))
       val sum= intRdd.filter { x => (x%2==0) }.sum()
       print(sum)
       
       val file = sc.textFile("/Users/nishi/Documents/workspace/spark_learn/src/main/resources/people.txt")
       file.cache()
       val fileMap = file.flatMap { x => x.split(",") }
       fileMap.toDebugString
       fileMap.foreach { println }
       fileMap.take(2)

       
  }
}