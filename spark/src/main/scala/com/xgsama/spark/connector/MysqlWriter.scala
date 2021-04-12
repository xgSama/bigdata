package com.xgsama.spark.connector

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * MysqlWriter
 *
 * @author xgSama
 * @date 2021/1/28 16:26
 */
object MysqlWriter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MysqlReader").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd1: RDD[(Int, String, Int)] =
      spark
        .sparkContext
        .makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))

    val source: DataFrame = rdd1.toDF("id", "name", "age")

    val url: String = "jdbc:mysql://47.103.218.168:3307/test?characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai"
    val user: String = "root"
    val password: String = "cyz19980815"

    // TODO 方式1：通用的方式  format指定写出类型
    source.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "cus")
      .mode(SaveMode.Append)
      .save()

    // TODO 方式2：通过JDBC方法
    val props: Properties = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    source.write
      .mode(SaveMode.Append)
      .jdbc(url, "cus", props)


    spark.close()
  }

}
