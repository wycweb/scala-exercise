package com.wycweb.bigdata.etl.common.excel

import java.util

import com.alibaba.excel.context.AnalysisContext
import com.alibaba.excel.event.AnalysisEventListener
import com.alibaba.excel.metadata.BaseRowModel

//easyexcel 插件基础类 目前无特殊处理
class ExcelListener[T <: BaseRowModel] extends AnalysisEventListener[T] {
  final private val rows = new util.ArrayList[T]

  override def invoke(`object`: T, context: AnalysisContext): Unit = {
    rows.add(`object`)
  }

  override def doAfterAllAnalysed(context: AnalysisContext): Unit = {
  }

  def getRows: util.List[T] = rows
}