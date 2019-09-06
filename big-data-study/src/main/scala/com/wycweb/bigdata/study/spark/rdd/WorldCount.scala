package com.wycweb.bigdata.study.spark.rdd

import com.wycweb.bigdata.common.config.BigDataEnvConfig

/**
  * @Description spark实现wordcount
  * 理解reduceByKey 和 groupByKey
  */
object WorldCount {

  val dataPath = "file:///private/var/data/movie/"
  private val sparkSession = BigDataEnvConfig.getSparksession
  private val sc = sparkSession.sparkContext


  def main(args: Array[String]): Unit = {

    val wordRDD = sc.textFile(dataPath + "wordcount.txt")
      .flatMap(line => line.split(" ")) //将数据 分隔 拍平 存储到rdd中
      .map(word => (word, 1)) //将数据转换成 (word,1) 的格式

    /**
      * 假如数据为 spark scala scala java java java
      * wordRDD得结果为
      * (spark,1) (scala,1) (scala,1) (java,1) (java,1) (java,1)
      * 使用reduceByKey 会按照key相同进行归类，然后使用lambda表达式进行相关操作
      * 使用groupByKey 生成（spark,(1)）（scala,(1,1)）（java,(1,1,1)）
      */
    val worldCount1 = wordRDD
      .reduceByKey(_ + _) //等价于.reduceByKey((x,y)=>x+y)
      .collect()

    val worldCount2 = wordRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
  }
}
