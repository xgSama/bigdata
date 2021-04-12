package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ESReader
 *
 * @author xgSama
 * @date 2021/1/27 9:52
 */
object ESReader {
  def main(args: Array[String]): Unit = {
    /*
    // TODO 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 如果查询的列名采用单引号，需要隐式转换，SparkSession对象必须使用val定义


    val options: Map[String, String] = Map(
      "es.index.auto.create" -> "true",
      "es.nodes" -> "dtbase",
      "es.nodes.wan.only" -> "true",
      "es.read.field.as.array.include" -> "tags",
      "es.port" -> "9200")

    val index: String = "zy_demo/_doc"

    val esDF: DataFrame = spark
      .read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load(index)

    esDF.show()


     */
  }
}
