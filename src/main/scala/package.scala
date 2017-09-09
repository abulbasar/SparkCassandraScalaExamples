package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object scala {

  val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")
    .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    .setIfMissing("spark.cassandra.auth.username", "cassandra")
    .setIfMissing("spark.cassandra.auth.password", "pass123")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val sql = spark.sql _
  val sparkConf = spark.sparkContext.getConf
}