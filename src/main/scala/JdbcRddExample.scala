import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author ollopollo
 */
object JdbcRddExample {
 def main(args: Array[String]) {
 
       val url="jdbc:mysql://nn01.itversity.com/retail_db"
       val username = "retail_dba"
       val password = "itversity"
       Class.forName("com.mysql.jdbc.Driver").newInstance
       
       val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
       val sc = new SparkContext(conf)
       val stringRdd= sc.parallelize(Array("java","scala","java csript"))
       val filteredRdd = stringRdd.filter { x => x.startsWith("j") }
       filteredRdd.foreach { println }
       
       
       //val myRDD = new JdbcRDD( sc, () => DriverManager.getConnection(url,username,password) ,
      //"select category_name from categories limit ?, ?",
      //1, 10, 2, r => r.getString("category_name") )
       
       //myRDD.foreach(println)
       //myRDD.saveAsTextFile("C:\\jdbcrddexample")
       
  }
}