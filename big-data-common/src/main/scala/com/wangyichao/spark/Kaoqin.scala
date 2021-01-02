package com.wangyichao.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object Kaoqin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("app")
      .getOrCreate()


    val input = spark.read
      .option("header", "true")
      .csv("/Users/wangyichao/Desktop/kaoqin.csv")

    val url = "jdbc:mysql://localhost:3306/cyclone_cap?useUnicode=true&characterEncoding=utf-8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "wangyichao")

    input.write.mode(SaveMode.Overwrite).jdbc(url, "attendance", prop)
  }

}
