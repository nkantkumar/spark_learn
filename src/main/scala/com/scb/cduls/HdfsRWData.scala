package com.scb.cduls

import org.apache.log4j.LogManager
import org.apache.spark.sql.{ SaveMode, SparkSession }
object HdfsRWData {
  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    val sparkSession = SparkSession.builder().appName("read-and-write-from-hdfs").master("local").getOrCreate()
    import sparkSession.implicits._

    val hdfs_master = "hdfs://localhost:9000/";
    // ====== Creating a dataframe with 1 partition
    val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)

    df.write.mode(SaveMode.Overwrite).parquet(hdfs_master + "nishi/testwiki")

    df.write.mode(SaveMode.Overwrite).csv(hdfs_master + "nishi/testwiki.csv")

    val df_parquet = sparkSession.read.parquet(hdfs_master + "nishi/testwiki")
    log.info(df_parquet.show())
    val df_csv = sparkSession.read.option("inferSchema", "true").csv(hdfs_master + "nishi/testwiki.csv")
    log.info(df_csv.show())
  }

}