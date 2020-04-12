package org.pcg.walrus.common.util

import org.apache.spark.SparkContext
import org.pcg.walrus.common.util.MathUtil.{mean, median, stdDev}
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.sql.functions._

/**
  * common math functions
  */
object WalrusUtil {

  /**
    * broadcast table
    */
  def broadcastTable(sc: SparkContext, df: Dataset[Row], table: String): Unit = sc.broadcast(df.as(table))

  /**
    * cache table
    */
  def cacheTable(sqlContext: SQLContext, df: Dataset[Row], table: String): Unit = {
    val tempTable = s"temp_$table"
    df.createOrReplaceTempView(tempTable)
    sqlContext.sql(s"CACHE TABLE $table AS SELECT * FROM $tempTable")
  }

  /**
    * calculate data skewness
    * skewness = 3 * (mean - median) / stand_dev
    */
  def skewness(df: Dataset[Row], key: String, fraction: Double): Double = {
    val s = df.sample(false, fraction).select(key).groupBy(key).count.select("count")
      .collect().map{ row => row.getLong(row.fieldIndex("count")).toDouble}
    val std = stdDev(s)
    std match {
      case 0 => Double.MaxValue
      case _ => Math.abs(3 * (mean(s) - median(s))) / std
    }
  }

  /**
    * return first {num} hot values in df
    */
  def hotVals(data: Dataset[Row], key: String, num: Int, fraction: Double): Array[String] = {
    data.sample(false, fraction).select(key).groupBy(key).count.orderBy(desc("count")).limit(num)
      .collect().map{ row => row.get(row.fieldIndex(key)).toString}
  }
}