package com.xgsama.spark.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * HiveReader
 *
 * @author xgSama
 * @date 2021/1/28 14:35
 */
object HiveReader {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    // TODO 创建环境对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("use xuange")
    // TODO 准备数据
    /*
    spark.sql("show databases").show()

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(" load data local inpath 'F:/z资料/大数据/Spark/spark-sql数据/user_visit_action.txt' into table user_visit_action")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql("load data local inpath 'F:/z资料/大数据/Spark/spark-sql数据/product_info.txt' into table product_info")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql("load data local inpath 'F:/z资料/大数据/Spark/spark-sql数据/city_info.txt' into table city_info")

    spark.sql(
      """
        |select * from city_info
        |""".stripMargin).show()



     */

    // 基本信息表
    spark.sql(
      """
        |select
        |	a.*,
        |	p.product_name,
        |	c.area,
        |	c.city_name
        |from user_visit_action a
        |join  product_info p on a.click_product_id=p.product_id
        |join  city_info c on a.city_id=c.city_id
        |where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

//    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF))


    spark.sql(
      """
        |select
        |	area,
        |	product_name,
        |	count(*) as clickCnt,
        | cityRemark(city_name) as city_remark
        |from t1 group by area, product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        | *,
        | rank() over( partition by area order by clickCnt) as rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select * from t3 where rank<=3
        |""".stripMargin).show(false)

    /*
    spark.sql(
      """
        |select
        | *
        |from
        |(
        |	select
        |		*,
        |		rank() over( partition by area order by clickCnt) as rank
        |	from
        |	(
        |		select
        |			area,
        |			product_name,
        |			count(*) as clickCnt
        |		from
        |		(
        |			select
        |				a.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from user_visit_action a
        |			join  product_info p on a.click_product_id=p.product_id
        |			join  city_info c on a.city_id=c.city_id
        |			where a.click_product_id > -1
        |		) t1 group by area, product_name
        |	) t2
        |) t3 where rank <= 3
        |""".stripMargin).show()

     */


    spark.close()

  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  /**
   * IN: 城市名称
   * BUF: 总点击量 ,Map[(city, cnt), (city, cnt)]
   */
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0L, mutable.Map[String, Long]())
    }

    // 更新缓冲区数据
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCount: Long = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCount)
      buff

    }

    // 合并缓冲区
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      val map1: mutable.Map[String, Long] = b1.cityMap
      val map2: mutable.Map[String, Long] = b2.cityMap

      b1.cityMap = map1.foldLeft(map2) {
        case (map, (city, cnt)) =>
          val newCount: Long = map.getOrElse(city, 0L) + cnt
          map.update(city, newCount)
          map
      }

      b1
    }

    //
    override def finish(reduction: Buffer): String = {
      val remarkList: ListBuffer[String] = ListBuffer[String]()

      val totalCnt: Long = reduction.total
      val cityMap: mutable.Map[String, Long] = reduction.cityMap

      // 降序排列
      val cityCntList: List[(String, Long)] = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }).take(2)

      val hasMore: Boolean = cityMap.size > 2

      var rsum: Long = 0L
      cityCntList.foreach {
        case (city, cnt) =>
          val r: Long = cnt * 100 / totalCnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
      }

      if (hasMore) {
        remarkList.append(s"其他 ${100 - rsum}%")
      }

      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
