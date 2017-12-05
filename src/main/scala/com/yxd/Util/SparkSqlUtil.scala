package com.yxd.Util

/**
 * Created by YXD on 2017/12/5.
 */
object SparkSqlUtil {
  val TABLE_NAME = "weblogs"
  val DATA_SEPARATE = "#@#"
  var count = 0

  def accumulation():Long = {
    synchronized{
      count += 1
    }
    count
  }
}
