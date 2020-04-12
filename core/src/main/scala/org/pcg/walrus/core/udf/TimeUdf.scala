package org.pcg.walrus.core.udf

import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat

import scala.util.Try

/**
  * common time udf: timestamp to date
  */
class TimeUdf extends WUDF {

  // register udf
  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("getDay", getDay(_:Long))
    sqlContext.udf.register("getYear", getYear(_:Long))
    sqlContext.udf.register("getQuarter", getQuarter(_:Long))
    sqlContext.udf.register("getMonth", getMonth(_:Long))
    sqlContext.udf.register("getHour", getHour(_:Long))
    sqlContext.udf.register("getMinute", getMinute(_:Long))
    sqlContext.udf.register("getWeek", getWeek(_:Long))
    sqlContext.udf.register("getWday", getWday(_:Long))
    sqlContext.udf.register("getTime", getTime(_:Long))
  }

  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 20170109
  def getDay(timestamp: Long) = format(timestamp, "yyyyMMdd", 19700101)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 2017
  def getYear(timestamp: Long) = format(timestamp, "yyyy", 1970)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 2017Q1
  def getQuarter(timestamp: Long) =  {
    Try {
      val format = new SimpleDateFormat("yyyyMMdd")
			val date = Integer.parseInt(format.format(timestamp * 1000))
			date/10000 + "Q" + ((date / 100 % 100 - 1)/ 3 + 1)
    }.getOrElse("1970Q1")
  }
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 201701
  def getMonth(timestamp: Long) = format(timestamp, "yyyyMM", 197001)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 18
  def getHour(timestamp: Long): Int = format(timestamp, "HH", -1)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 37
  def getMinute(timestamp: Long): Int = format(timestamp, "m", -1)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 2
  def getWeek(timestamp: Long): Int = format(timestamp, "w", -1)
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 0
  def getWday(timestamp: Long): Int = format(timestamp, "u", -1) - 1
  
  // 1483958256(Mon Jan  9 18:37:36 CST 2017) > 2
  def getTime(timestamp: Long): String = format(timestamp, "yyyyMMdd HH:mm:ss", "19700101 00:00:00")

  private[udf] def format(timestamp: Long, format: String, defaultVal: String) = {
    Try {
      new SimpleDateFormat(format).format(timestamp * 1000)
    }.getOrElse(defaultVal)
  }
  
  private[udf] def format(timestamp: Long, format: String, defaultVal: Int): Int = {
    Try {
      Integer.parseInt(new SimpleDateFormat(format).format(timestamp * 1000))
    }.getOrElse(defaultVal)
  }
}