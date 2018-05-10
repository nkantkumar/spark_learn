package com.scb.cduls
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
object GroupByTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
  

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect().foreach(println)

    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect().foreach(println)
  }
}