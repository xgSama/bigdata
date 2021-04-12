package com.xgsama.spark.connector

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * eerer
 *
 * @author xgSama
 * @date 2021/2/5 16:54
 */
object eerer {
  def main(args: Array[String]): Unit = {
    //Reader


    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    //读
    val dataFrame: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.103.201:3306/tags_dat?characterEncoding=utf8&useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "tbl_goods")
      .load()


    spark.sql("use xingran")
    dataFrame.createOrReplaceTempView("goods22")
//    spark.sql("insert into tbl_goods select * from goods22")
    spark.sql("desc formatted tbl_goods").show()

  }

}
