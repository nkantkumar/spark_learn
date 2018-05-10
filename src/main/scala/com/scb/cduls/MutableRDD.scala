package com.scb.cduls
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import scala.util.Random
object MutableRDD {
   def main(args: Array[String]) {
   val conf = new SparkConf().setAppName("Immutable RDD test").setMaster("local")
   val sc = new SparkContext(conf)

   // start with a sequence of 10,000 zeros
   val zeros = Seq.fill(10000)(0)

   // create a RDD from the sequence, and replace all zeros with random values
   val randomRDD = sc.parallelize(zeros).map(x=>Random.nextInt())
   randomRDD.take(10).foreach(println)

   // filter out all non-positive values, roughly half the set
   val filteredRDD = randomRDD.filter(x=>x>0)
   

   // count the number of elements that remain, twice
   val count1 = filteredRDD.count()
   val count2 = filteredRDD.count()

   // Since filteredRDD is immutable, this should always pass, right? 
   //assert(count1 == count2, "\nMismatch!  count1="+count1+" count2=+count2)

   System.out.println("Program completed successfully")
 }
}