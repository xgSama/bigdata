package com.xgsama.spark.connector

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * MysqlReader
 *
 * @author xgSama
 * @date 2021/1/28 15:57
 */
object MysqlReader {

  val QUERY: String = "query"
  val DBTABLE: String = "dbtable"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MysqlReader").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val url: String = "jdbc:mysql://47.103.218.168:3307/test?characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai"
    val user: String = "root"
    val password: String = "cyz19980815"
    val sql =  "select * from customers where cust_id < 10003"

    // TODO 方式1：通用的load方法读取
    val options: Map[String, String] = Map(
      "url" -> url,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> user,
      "password" -> password
    )


    val jdbcDF1: DataFrame = spark.read
      .format("jdbc")
      .options(options)
      .option("query", sql)
      .load()
    jdbcDF1.show()



    val options2: Map[String, String] = Map(
      "url" -> url,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> user,
      "password" -> password,
      "dbtable" -> "customers121"
      )

    jdbcDF1.write
      .format("jdbc")
      .options(options2)
      .mode(SaveMode.Ignore)
      .save()


    // TODO 方式2：通用的JDBC方法读取
    val props: Properties = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("password", password)

    val jdbcDF2: DataFrame = spark.read.jdbc(url, "customers121", props)
    jdbcDF2.show()





    spark.close()

  }

}
