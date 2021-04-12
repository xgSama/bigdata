package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * ESWriter
 *
 * @author xgSama
 * @date 2021/1/28 15:10
 */
object ESWriter {

  def main(args: Array[String]): Unit = {
    /*
    // TODO 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 如果查询的列名采用单引号，需要隐式转换，SparkSession对象必须使用val定义
    import spark.implicits._

    val options: Map[String, String] = Map(
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "dtbase",
      "es.port" -> "9200",
      "es.mapping.id" -> "id"
    )


    val rdd1: RDD[(Int, String, Int)] =
      spark
        .sparkContext
        .makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))

    val source: DataFrame = rdd1.toDF("id", "name", "age")

    source
      .write
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save("spark/_doc")

    spark.close()

     */
  }


}
