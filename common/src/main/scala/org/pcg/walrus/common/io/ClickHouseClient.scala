package org.pcg.walrus.common.io

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}
import ru.yandex.clickhouse.settings.ClickHouseProperties

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.util.Try

import java.sql.{PreparedStatement, ResultSetMetaData}

/**
  * click house client
  */
object ClickHouseClient extends Serializable {

  // wait when server is busy
  private val WAIT_TIME = 10
  /**
    * ck do not support: PreparedStatementImpl.getMetaData
    * TODO get result metadata without executing query
    */
  def metadata(connection: ClickHouseConnection, sql: String): ResultSetMetaData = {
    //    throw new Exception("java.sql.SQLFeatureNotSupportedException: ClickHousePreparedStatementImpl.getMetaData is not supported!")
    val limitSql = s"$sql LIMIT 0"
    println(s"exe query: $limitSql")
    connection.createStatement().executeQuery(limitSql).getMetaData
  }

  /**
    * fake a rdd -> query ck on executor and parse to dataset
    * TODO: JUST query ck one time for metadata
    */
  def query(session: SparkSession,
            sql: String,
            ip: String,
            port: Int,
            db: String,
            user: String,
            pwd: String,
            timeout: Int = 60) :Dataset[Row] = {
    val fakeRdd = session.sparkContext.parallelize(List(Row.fromSeq(List(1))), 1)
    val rdd: RDD[Row] = fakeRdd.mapPartitions { p =>
      val connection = connect(ip, port, db, user, pwd, timeout)
      val rs = connection.createStatement().executeQuery(sql)
      val metadata = rs.getMetaData()
      val rows = ArrayBuffer.empty[Row]
      while (rs.next()) {
        rows += HdfsClient.readRow(metadata, rs)
      }
      rows.iterator
    }
    val connection = connect(ip, port, db, user, pwd, timeout)
    val schema = HdfsClient.parseSchema(metadata(connection, sql))
    val result = session.sqlContext.createDataFrame(rdd, schema)
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result
  }

  /**
    * drop table partition:
    * 1 get all ip from click house cluster
    * 2 foreach ip and drop partition from local table
    */
  def dropPartition(servers: String,
        table: String,
        partitions: String,
        db: String,
        user: String,
        pwd: String,
        timeout: Int = 60) = {
    servers.split(",").foreach { ipPort => {
      val info = ipPort.split(":")
      val localTable = s"${table}_local"
      val connection = connect(info(0), info(1).toInt, db, user, pwd, timeout)
      partitions.foreach(par => {
        val localSql = s"alter table $localTable drop partition ($par)"
        println(s"$ipPort alter table $localTable drop partition ($par)")
        Try { connection.createStatement().executeQuery(localSql) }.getOrElse(null)
      })
    }}
  }

  /**
    * insert spark dateset into click house
    * @confParam zkname click house zkname
    * @confParam db click house db
    * @confParam table click house table
    * @confParam batch insert batch size
    */
  def insert(data: Dataset[Row],
             spark:SparkSession,
             serverStr: String,
             table: String,
             db: String,
             user: String,
             pwd: String,
             batchSize: Int = 1000,
             minBatchInterval: Int = 1,
             timeout: Int = 60) = {
    implicit val rddEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    val schema = data.schema.fields.map { f => f.name }
    val types = data.schema.fields.map { f => f.dataType.typeName }
    val servers = serverStr.split(",")
    // map partitions
    val result = data.repartition(servers.length).rdd.mapPartitionsWithIndex { (index, rows) => {
      // connection to click house
      //      val ipPort = randomServer(conf.getOrElse("server", ""))
      val ipPort = servers(index).split(":")
      val connection = connect(ipPort(0), ipPort(1).toInt, db, user, pwd, timeout)
      // insert batch size
      val asks = schema.map { x => "?" }
      val sql = s"INSERT INTO $table (${schema.mkString(",")}) VALUES(${asks.mkString(",")})"
      println(s"click house insert sql: $sql")
      val pstmt = connection.prepareStatement(sql)
      var rowCount = 0
      var rs = ArrayBuffer.empty[Row]
      var start = System.currentTimeMillis()
      while(rows.hasNext) {
        val row = rows.next()
        rs += row
        row.schema.fields.map { x => {
          val index = row.fieldIndex(x.name)
          x.dataType.typeName.toLowerCase().replaceAll("\\(.*\\)", "") match {
            case "integer" => pstmt.setInt(index + 1, Try { row.getInt(index) }.getOrElse(0))
            case "long" => pstmt.setLong(index + 1, Try { row.getLong(index) }.getOrElse(0))
            case "double" => pstmt.setDouble(index + 1, Try { row.getDouble(index) }.getOrElse(0))
            case "decimal" => pstmt.setDouble(index + 1, Try { row.getDouble(index) }.getOrElse(0))
            case _ => pstmt.setString(index + 1, Try { row.getString(index) }.getOrElse(""))
          }
        }}
        pstmt.addBatch()
        rowCount += 1
        //
        if (rowCount % batchSize == 0) {
          executInsert(pstmt, minBatchInterval, start, rowCount)
          rowCount = 0
          start = System.currentTimeMillis()
        }
      }
      if (rowCount > 0) executInsert(pstmt, minBatchInterval, start, rowCount)
      connection.close()
      rs.iterator
    }}
    // trigger action
    result.count()
  }

  // execute insert every interval, retry when server is busy
  private def executInsert(pstmt: PreparedStatement, interval: Int, start: Long, rowSize: Int) = {
    try {
      var end = System.currentTimeMillis()
      while((end - start) < (interval * 1000)) {
        Thread.sleep(10)
        end = System.currentTimeMillis()
      }
      pstmt.executeBatch()
      val finish = System.currentTimeMillis()
      println(s"[${new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new java.util.Date())}]executeBatch $rowSize cost: ${finish-end}")
    } catch {
      case unknown: Throwable => {
        Thread.sleep(WAIT_TIME * 1000)
        pstmt.executeBatch()
      }
    }
  }
  // connect to click house db
  private def connect(ip: String,
              port: Int,
              db:String,
              user: String,
              pwd: String,
              timeout: Int): ClickHouseConnection = {
    val connectUrl = s"jdbc:clickhouse://$ip:$port/$db?user=$user&password=$pwd"
    println(s"connect url: $connectUrl")
    // conf properties
    val properties = new ClickHouseProperties()
    properties.setTimeToLiveMillis(timeout * 1000)
    properties.setKeepAliveTimeout(timeout * 1000)
    properties.setSocketTimeout(timeout * 1000)
    new ClickHouseDataSource(connectUrl, properties).getConnection()
  }
}