package org.pcg.walrus.common.io

import java.util.Properties
import java.sql.{Driver, Connection, ResultSet, ResultSetMetaData, SQLException}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * kylin client
  */
object KylinClient extends Serializable {

  /**
    * fake a rdd -> query ck on executor and parse to dataset
    * TODO: JUST query ck one time for metadata
    */
  def query(session: SparkSession, sql: String, url: String, user: String, password: String) :Dataset[Row] = {
    val fakeRdd = session.sparkContext.parallelize(List(Row.fromSeq(List(1))), 1)
    val rdd: RDD[Row] = fakeRdd.mapPartitions { p =>
      val connection = connect(url, user, password)
      val rs = connection.createStatement().executeQuery(sql)
      val metadata = rs.getMetaData()
      val rows = ArrayBuffer.empty[Row]
      while (rs.next()) {
        rows += HdfsClient.readRow(metadata, rs)
      }
      rows.iterator
    }
    val connection = connect(url, user, password)
    val schema = HdfsClient.parseSchema(metadata(connection, sql))
    val result = session.sqlContext.createDataFrame(rdd, schema)
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result
  }

  /**
    * get connection
    */
  private def connect(url: String, user: String, password: String): Connection = {
    var driver: Driver = Class.forName("org.apache.kylin.jdbc.Driver").newInstance.asInstanceOf[Driver]
    val info = new Properties()
    info.put("user", user)
    info.put("password", password)
    driver.connect(url, info)
  }

  @throws[SQLException]
  def query(conn: Connection, sql: String): ResultSet = {
    val statement = conn.prepareStatement(sql)
    statement.executeQuery
  }

  @throws[SQLException]
  private def metadata(conn: Connection, sql: String): ResultSetMetaData = {
    val meta = query(conn, s"$sql LIMIT 0").getMetaData
    meta
  }
}