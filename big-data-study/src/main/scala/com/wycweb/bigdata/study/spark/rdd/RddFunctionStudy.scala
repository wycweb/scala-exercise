package com.wycweb.bigdata.study.spark.rdd

import com.wycweb.bigdata.common.config.BigDataEnvConfig

/**
  * RDD操作常见函数测试
  */
object RddFunctionStudy {

  private val sparkSession = BigDataEnvConfig.getSparksession

  def main(args: Array[String]): Unit = {

    val wordRDD = sparkSession.sparkContext
      .parallelize(Array("spark", "scala", "scala", "java", "java", "java"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val keys = wordRDD.keys //输出键值对rdd里有的key
    val values = wordRDD.values //输出键值对rdd里有的value
    val sortByKey = wordRDD.sortByKey() //按照key默认升序排序，传入false 按降序排序
    val sortByCondition = wordRDD.sortBy(_._2, false) //按照给定条件进行排序
    val mapValues = wordRDD.mapValues(x => x + 1) //对键值对rdd里的value进行相应的操作


    val joinRDD = sparkSession.sparkContext.parallelize(
      Array(("spark", "good"), ("scala", "wonderful"))
    )

    //将key相同的关联在一起
    val joinResult = wordRDD.join(joinRDD)

    //结果 (scala,(2,wonderful)) (spark,(1,good))
    joinResult.foreach(println)
    demo()
  }

  /**
    * 求图书的平均销量
    */
  def demo(): Unit = {

    //key 标识图书名称 value 标识某天图书销量
    val rddArray: Array[(String, Int)] = Array(
      ("spark", 2),
      ("hadoop", 6),
      ("hadoop", 4),
      ("spark", 6)
    )
    val rdd = sparkSession.sparkContext
      .parallelize(rddArray)

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
}
