package com.scb.cduls.sql

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
object StreamToDF {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StatefulNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("StreamToDF")
   
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")

    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}