package main.scala

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.datastax.spark.connector.cql.CassandraConnector


object CassandraClient {
  
  def main(args: Array[String]): Unit = {
    val path = "/etc/passwd"
    val conf = new SparkConf()

    conf.setAppName("sample demo")
    conf.setMaster("local[*]")
    
    
    /* Cassandra properties */
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val df = sqlContext
              .read
              .format("org.apache.spark.sql.cassandra")
              .options(Map( "table" -> "stocks", "keyspace" -> "lab"))
              .load()
     df.show()
     
     val df_with_year_mmonth = df
                    .withColumn("year", year($"date"))
                    .withColumn("month", month($"date"))
                    
     df_with_year_mmonth.write
            .mode("append")
            .format("org.apache.spark.sql.cassandra")
            .options(Map( "table" -> "stocks_by_year_month", "keyspace" -> "lab"))
            .save()

  }
}
