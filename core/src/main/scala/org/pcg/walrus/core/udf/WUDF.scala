package org.pcg.walrus.core.udf

import org.apache.spark.sql.SQLContext

/**
  * walrus udf
  */
trait WUDF extends Serializable {

  /**
    * register your udf
    */
  def register(sqlContext: SQLContext): Unit
}