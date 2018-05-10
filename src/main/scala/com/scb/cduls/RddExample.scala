import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author nkantkumar
 */
object RddExample {
 def main(args: Array[String]) {
       val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
       val sc = new SparkContext(conf)
       val stringRdd= sc.parallelize(Array("java","scala","java csript"))
       val filteredRdd = stringRdd.filter { x => x.startsWith("j") }
       filteredRdd.foreach { println }
       
       val valPairRdd = stringRdd.map { x => (x,1) }
       val wordCount = valPairRdd.reduceByKey((x,y)=>x+y)
       wordCount.foreach { println }
       
       

       
  }
}