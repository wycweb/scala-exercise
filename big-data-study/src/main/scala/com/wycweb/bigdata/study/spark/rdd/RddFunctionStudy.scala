package com.wycweb.bigdata.study.spark.rdd

import com.wycweb.bigdata.common.config.BigDataEnvConfig

import scala.util.parsing.json.JSON

/**
  * RDD操作常见函数测试
  */
object RddFunctionStudy {

  private val sparkSession = BigDataEnvConfig.getSparksession
  private val sc = sparkSession.sparkContext

  /**
    * 使用parallelize创建RDD 也 可以使用makeRDD来创建RDD。
    * 通过查看源码可以发现，makeRDD执行的时候，也是在调用parallelize函数，二者无区别。
    *
    * 通过 .textFile 可以通过文件读取项目路径 和 hdfs 文件路径
    *
    * makeRDD 和 parallelize 第二个参数为处理的并行度数量，
    * 不给定时，默认值为 通过
    * conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2)) 获取
    * 即 获取 spark.default.parallelism 参数值
    * 当参数值存在时，使用 spark.default.parallelism 配置的参数
    * 当参数不存在时，比较系统总共可用核数 和 2 ，哪个大使用哪个
    *
    * saveRDD 函数存储的分区数，即数据文本数量，取决于 运行的并行度
    */
  def main(args: Array[String]): Unit = {

    createRDD() //创建RDD
    saveRDD() //存储RDD
    demo() //RDD综合使用，计算哪个图书日销量最大
    jsonDemo() //使用scala原生函数，处理json格式数据
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

  def createRDD(): Unit = {
    val wordRDDTemp = sc.parallelize(Array("spark", "scala", "scala", "java", "java", "java"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

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
  }

  //将RDD数据存储
  def saveRDD(): Unit = {
    //使用mdkeRDD来创建RDD
    val wordRDD = sc.makeRDD(Array("spark", "scala", "scala", "java", "java", "java"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordRDD.saveAsTextFile("/Users/wangyichao/Desktop/RDD")
  }

}
