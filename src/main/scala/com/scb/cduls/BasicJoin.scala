package com.scb.cduls

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
object BasicJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30", "SB10010,7000", "CR10001,7000", "SB10002,-10")

    val acTransRDD = sc.parallelize(acTransList)

    val goodTransRecords = acTransRDD.filter(_.split(",")(1).toDouble > 0).filter(_.split(",")(0).startsWith("SB"))

    val highValueTransRecords = goodTransRecords.filter(_.split(",")(1).toDouble > 1000)
    val badAmountLambda = (trans: String) => trans.split(",")(1).toDouble <= 0
    val badAcNoLambda = (trans: String) => trans.split(",")(0).startsWith("SB") == false
    val badAmountRecords = acTransRDD.filter(badAmountLambda)
    val badAccountRecords = acTransRDD.filter(badAcNoLambda)
    val badTransRecords = badAmountRecords.union(badAccountRecords)

    val sumAmount = goodTransRecords.map(_.split(",")(1).toDouble).reduce(_ + _)
    val maxAmount = goodTransRecords.map(_.split(",")(1).toDouble).reduce((a, b) => if (a > b) a else b)
    val minAmount = goodTransRecords.map(_.split(",")(1).toDouble).reduce((a, b) => if (a < b) a else b)

    val combineAllElements = acTransRDD.flatMap(trans => trans.split(","))
    val allGoodAccountNos = combineAllElements.filter(_.startsWith("SB"))
    combineAllElements.collect()
    allGoodAccountNos.distinct().collect()
  }
}