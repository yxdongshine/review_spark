package com.yxd.Util

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by YXD on 2017/12/5.
 */
object DateUtil {
  val TO_MONTH = "yyyyMM";
  val TO_DAY = "yyyyMMdd";
  val TO_HOUR = "yyyyMMddHH";
  val TO_MINUTE = "yyyyMMddHHmm";
  val TO_SECOND = "yyyyMMddHHmmss";
  val TO_MILLISECOND = "yyyyMMddHHmmssSSS";

  val TO_MONTH_LINE = "yyyy-MM";
  val TO_DAY_LINE = "yyyy-MM-dd";
  val TO_HOUR_LINE = "yyyy-MM-dd HH";
  val TO_MINUTE_LINE = "yyyy-MM-dd HH:mm";
  val TO_SECOND_LINE = "yyyy-MM-dd HH:mm:ss";
  val TO_MILLISECOND_LINE = "yyyy-MM-dd HH:mm:ss.SSS";

  val TO_MONTH_SLASH = "yyyy/MM";
  val TO_DAY_SLASH = "yyyy/MM/dd";
  val TO_HOUR_SLASH = "yyyy/MM/dd HH";
  val TO_MINUTE_SLASH = "yyyy/MM/dd HH:mm";
  val TO_SECOND_SLASH = "yyyy/MM/dd HH:mm:ss";
  val TO_MILLISECOND_SLASH = "yyyy/MM/dd HH:mm:ss.SSS";

  val TO_MONTH_DOT = "yyyy.MM";
  val TO_DAY_DOT = "yyyy.MM.dd";
  val TO_MINUTE_DOT = "yyyy.MM.dd HH:mm";
  val TO_SECOND_DOT = "yyyy.MM.dd HH:mm:ss";
  val TO_MILLISECOND_DOT = "yyyy.MM.dd HH:mm:ss.SSS";

  val ONLY_YEAR = "yyyy";
  val ONLY_MONTH = "MM";
  val ONLY_TIME = "HHmmss";
  val ONLY_TIME_COLON = "HH:mm:ss";

  val MONTH_AND_DAY = "MMdd";
  val MONTH_AND_DAY_LINE = "MM-dd";
  val MONTH_AND_DAY_SLASH = "MM/dd";
  val MONTH_AND_DAY_DOT = "MM.dd"

  /**
   * 获取当前时间
   * @return
   * @author yxd
   */
  def now():Date = {
      new Date()
  }

  /**
   * 获取当前毫秒数
   * @return
   * @author yxd
   */
  def nowTime():Long = {
     now().getTime()
  }

  /**
   * 将Date转换成指定格式的字符串
   * @param date
   * @param format
   * @return
   * @author OL
   */
  def format(date:Date,format:String ):String = {
    val sdf = new SimpleDateFormat(format);
    sdf.format(date)
  }


  /**
   * 获取数据前缀
   * @return
   */
  def getDataPrefix():String = {
    format(now(),TO_SECOND_LINE).substring(0,4)
  }


  def main(args: Array[String]) {
    println(getDataPrefix)
  }
}
