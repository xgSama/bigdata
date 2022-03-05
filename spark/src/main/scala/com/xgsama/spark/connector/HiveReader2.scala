package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * HiveReader
 *
 * @author xgSama
 * @date 2021/1/28 14:35
 */
object HiveReader2 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "admin")

    // TODO 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("create table if not exists abc(id int, name string) partitioned by (pt string)")

    spark.sql("insert overwrite table abc partition(pt='0303') values(1, 'sss'), (2, 'sca')")

    spark.sql("select * from abc").show()

    spark.close()
  }
}
