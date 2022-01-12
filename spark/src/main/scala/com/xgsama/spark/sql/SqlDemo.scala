package com.xgsama.spark.sql

import com.xgsama.spark.udf.MyUDF
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StringType}


/**
 * SqlDemo
 *
 * @author xgSama
 * @date 2021/2/2 16:59
 */
object SqlDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("UDF").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    userDF.show

    userDF.createOrReplaceTempView("user")

    spark.udf.register("concat1", (a: String) => {
      a.length
    })

  }
}
