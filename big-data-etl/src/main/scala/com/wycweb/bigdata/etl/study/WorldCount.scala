package com.wycweb.bigdata.etl.study

import com.wycweb.bigdata.common.config.BigDataEnvConfig

object WorldCount {
  private val sparkSession = BigDataEnvConfig.getSparksession

  val dataPath = "file:///private/var/data/movie/"

  def main(args: Array[String]): Unit = {
    val usersRDD = sparkSession.sparkContext.textFile(dataPath + "test.txt")

    val worldCount = usersRDD
      .flatMap(line => line.split(" ")) //将数据分隔拍平存储到rdd中
      .map(word => (word, 1)) //将数据转换成 (word,1) 的格式
      .reduceByKey((a, b) => a + b)
      .collect()

    worldCount.foreach(println)
  }
}
