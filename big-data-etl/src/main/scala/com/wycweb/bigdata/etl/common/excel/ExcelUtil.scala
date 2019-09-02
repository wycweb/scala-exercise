package com.wycweb.bigdata.etl.common.excel

import java.io.InputStream
import java.util

import com.alibaba.excel.ExcelReader
import com.alibaba.excel.metadata.{BaseRowModel, Sheet}

object ExcelUtil {

  /**
    * 根据定义的excel表头，读取excel 中的数据，返回arrayList格式的数据
    *
    * @param sheetNo     excel sheet编号，编号从1开始
    * @param headLineMun excel第一行数据开始，含表头，编号从1开始，读取出的数据不包含表头
    * @param inputStream 字节流 可传入本地的流 也可以为hdfs文件流
    * @param clazz       复写easyexcel 的 BaseRowModel 类，不同excel需进行不同的定义
    * @return arrayList格式的数据
    */
  def readExcel[T <: BaseRowModel](sheetNo: Integer, headLineMun: Integer, inputStream: InputStream, clazz: Class[_ <: BaseRowModel]): util.List[T] = {

    if (null == inputStream) throw new NullPointerException("the inputStream is null!")

    val listener: ExcelListener[T] = new ExcelListener[T]
    val reader: ExcelReader = new ExcelReader(inputStream, null, listener)
    reader.read(new Sheet(sheetNo, headLineMun, clazz))
    listener.getRows
  }
}
