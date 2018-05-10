package com.scb.cduls
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
object BasicFunction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val acMasterList = Array("SB10001,Roger,Federer", "SB10002,Pete,Sampras", "SB10003,Rafael,Nadal", "SB10004,Boris,Becker",  "SB10005,Ivan,Lendl")
    val acBalList = Array("SB10001,50000", "SB10002,12000",   "SB10003,3000", "SB10004,8500", "SB10005,5000")

    val acMasterRDD = sc.parallelize(acMasterList)
    val acBalRDD = sc.parallelize(acBalList)
    val acMasterTuples = acMasterRDD.map(master => master.split(",")).map(masterList => (masterList(0), masterList(1) + " " +   masterList(2)))
    acMasterTuples.collect().foreach(println)
    
    val acBalTuples = acBalRDD.map(trans =>   trans.split(",")).map(transList => (transList(0), transList(1)))
    acBalTuples.collect().foreach(println)
    
    val acJoinTuples =  acMasterTuples.join(acBalTuples).sortByKey().map{case (accno, (name, amount)) => (accno, name,amount)}
    acJoinTuples.collect().foreach(println)
    
    val acNameAndBalance = acJoinTuples.map{case (accno, name,amount) =>   (name,amount)}
    
    val acTuplesByAmount = acBalTuples.map{case (accno, amount) =>   (amount.toDouble, accno)}.sortByKey(false)
    acTuplesByAmount.first()
    acTuplesByAmount.take(3)
    
    acBalTuples.countByKey()
    acBalTuples.foreach(println)
    val balanceTotal = sc.accumulator(0.0, "Account Balance Total")
    acBalTuples.map{case (accno, amount) => amount.toDouble}.foreach(bal => balanceTotal += bal)
    balanceTotal.value
    
    
  }
  
}