package com.xgsama.spark.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * WordCount
 *
 * @author xgSama
 * @date 2021/1/28 15:48
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // TODO Spark - WordCount
    // Spark是一个计算框架

    // TODO 1. 准备Spark环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")

    // TODO 2. 建立和Spark的连接
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 3. 实现业务操作
    // TODO 3.1 读取指定目录下的数据文件（多个）
    // RDD：更适合并行计算的数据模型
    val fileRDD: RDD[String] = sc.textFile("input/word.txt")

    // TODO 3.2 将读取的内容进行扁平化操作
    // val wordRDD: RDD[String] = fileRDD.flatMap(line => {line.split(" ")})
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    // TODO 3.3 将分词后的数据进行结构转换
    // word => (word, 1)
    val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))

    // TODO 3.4 将转换后的数据进行分组聚合
    // reduceByKey方法的作用表示根据数据key进行分组，然后对value进行统计聚合
    val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)


    // TODO 3.5 将聚合的结果打印到控制台
    val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()

    println(wordCountArray.mkString(","))


    // TODO 4. 释放连接
    sc.stop()
  }

}
