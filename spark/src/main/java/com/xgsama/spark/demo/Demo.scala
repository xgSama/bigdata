package com.xgsama.spark.demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Demo
 *
 * @author : xgSama 
 * @date : 2021/11/5 18:24:26
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val confs: SparkConf = new SparkConf().setMaster("local[*]").setAppName("jobs")
    val sc = new SparkContext(confs)
    sc.setLogLevel("ERROR")
    val spark_session: SparkSession = SparkSession.builder()
      .appName("jobs").config(confs).getOrCreate()

    // 设置支持笛卡尔积 对于spark2.0来说
    spark_session.conf.set("spark.sql.crossJoin.enabled", true)

    //以jdbc方式连接mysql
    val url = "jdbc:mysql://172.16.101.50:3306/metastore"

    //设置用户名和密码信息
    val prop = new java.util.Properties
    prop.setProperty("user", "drpeco")
    prop.setProperty("password", "DT@Stack#123")

    //创建sqlContext对象
    val df1: DataFrame = spark_session.read.jdbc(url, "tbls", prop)
    val df2: DataFrame = spark_session.read.jdbc(url, "dbs", prop)
    val df3: DataFrame = spark_session.read.jdbc(url, "columns_v2", prop)

    //注册成spark临时表
    df1.createOrReplaceTempView("data1")
    df2.createOrReplaceTempView("data2")
    df3.createOrReplaceTempView("data3")

    //统计每一个库/表/字段信息
    val MySQL: String = "select tbs.`OWNER`,dbs.`NAME`,tbs.TBL_NAME,cv.COLUMN_NAME,cv.TYPE_NAME " +
      "FROM data1 tbs " +
      "INNER JOIN data2 dbs ON dbs.DB_ID = tbs.DB_ID " +
      "INNER JOIN data3 cv ON cv.CD_ID = tbs.TBL_ID"

    val df: DataFrame = spark_session.sql(MySQL)


    val frames = List(df)

    for (elem <- frames) {
      print(elem)
    }
    df.show(100)
    //    df.select("COLUMN_NAME").show()
  }
}
