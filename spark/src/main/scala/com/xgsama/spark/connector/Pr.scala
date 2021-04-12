package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Pr
 *
 * @author xgSama
 * @date 2021/2/5 14:36
 */
object Pr {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MysqlReader").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val frame: DataFrame = spark.read.csv("input/test1.csv/part-00000-096d34b6-72a1-4816-b29b-e2cdfaa1fec6-c000.csv")

    frame.show()
  }

}
