package com.scb.tem.poc
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SQLContext
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector._
object CassandraConnector {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[*]")
      .setIfMissing("hive.execution.engine", "spark")
      .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
      .setIfMissing("spark.cassandra.connection.port", "9042")
   

    val sc = new SparkContext( sparkConf)
    val table = sc.cassandraTable("finance", "employee")
    println(table.count)

  }
}