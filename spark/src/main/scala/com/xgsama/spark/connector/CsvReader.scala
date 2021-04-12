package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * CsvReader
 *
 * @author xgSama
 * @date 2021/2/5 13:45
 */
object CsvReader {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MysqlReader").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val options: Map[String, String] = Map(
      "delimiter" -> "|",
      "header" -> "true",
      "inferSchema" -> "true",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace " -> "true"
    )

    val csvDF: DataFrame = spark.read
      .format("csv")
      .options(options)
      .schema(ScalaReflection.schemaFor[User].dataType.asInstanceOf[StructType])
      .load("input/test.csv")

    val rdd1: RDD[(Int, String, Int)] =
      spark
        .sparkContext
        .makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20), (11, "sss", 8)))

    val source: DataFrame = rdd1.toDF("id", "name", "age")
    source.write
      .mode(SaveMode.Append)
      .format("csv")
      .options(options)
      .save("input/test1.csv")



    csvDF.show()
    csvDF.printSchema()

    spark.close()

  }

  case class User(id: Int, name: String, age: Int)

}
