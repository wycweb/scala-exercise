package com.wycweb.bigdata.study.spark.rdd

import com.wycweb.bigdata.common.config.BigDataEnvConfig

import scala.util.parsing.json.JSON

/**
  * RDD操作常见函数测试
  */
object RddFunctionStudy {

  private val sparkSession = BigDataEnvConfig.getSparksession
  private val sc = sparkSession.sparkContext

  def main(args: Array[String]): Unit = {

    //使用parallelize创建RDD 也 可以使用makeRDD来创建RDD
    //    val wordRDD = sc.parallelize(Array("spark", "scala", "scala", "java", "java", "java"))
    //      .map(word => (word, 1))
    //      .reduceByKey(_ + _)

    //使用mdkeRDD来创建RDD
    val wordRDD = sc.makeRDD(Array("spark", "scala", "scala", "java", "java", "java"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val keys = wordRDD.keys //输出键值对rdd里有的key
    val values = wordRDD.values //输出键值对rdd里有的value
    val sortByKey = wordRDD.sortByKey() //按照key默认升序排序，传入false 按降序排序
    val sortByCondition = wordRDD.sortBy(_._2, false) //按照给定条件进行排序
    val mapValues = wordRDD.mapValues(x => x + 1) //对键值对rdd里的value进行相应的操作


    val joinRDD = sc.parallelize(
      Array(("spark", "good"), ("scala", "wonderful"))
    )

    //将key相同的关联在一起
    val joinResult = wordRDD.join(joinRDD).collect()

    //结果 (scala,(2,wonderful)) (spark,(1,good))
    joinResult.foreach(println)
    demo()
    jsonDemo()
  }

  /**
    * @Description 求图书的平均销量
    */
  def demo(): Unit = {

    //key 标识图书名称 value 标识某天图书销量
    val rddArray: Array[(String, Int)] = Array(
      ("spark", 2),
      ("hadoop", 6),
      ("hadoop", 4),
      ("spark", 6)
    )

    val rdd = sc.parallelize(rddArray)

    val result = rdd.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect()

    /**
      * 结果
      * (spark,4)
      * (hadoop,5)
      */
    result.foreach(println)
  }

  /**
    * @Description 读取json格式的数据并通过JSON.parseFull函数解析json格式数据
    */
  def jsonDemo(): Unit = {
    val jsonStr = sc.textFile("file:///Users/wangyichao/Desktop/people.json")

    val result = jsonStr.map(s => JSON.parseFull(s))

    result.foreach({ r =>
      r match {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("Parsing failed")
        case other => println("Unknown data structure:" + other)
      }
    })
  }
}
