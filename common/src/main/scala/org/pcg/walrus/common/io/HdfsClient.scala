package org.pcg.walrus.common.io

import java.sql.{ResultSet, ResultSetMetaData, Types}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * <code>HdfsClient</code> </br>
 * common functions for hdfs
 */
object HdfsClient {
  
  /**
   * if path exist
   */
  def exists(conf: Configuration, path: String): Boolean = {
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new org.apache.hadoop.fs.Path(path))
  }
  
  /**
   * list file status match pattern
   */
  def globStatus(conf: Configuration, path: String): Array[org.apache.hadoop.fs.FileStatus] = {
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.globStatus(new org.apache.hadoop.fs.Path(path))
  }

  /**
    * load parquet
    */
  def readParquet(sqlContext: SQLContext, path: String): Dataset[Row] = {
    sqlContext.read.parquet(path)
  }

  /**
    * load csv
    */
  def readCsv(sc: SparkContext, sqlContext: SQLContext, schema: String, path: String, delim: String): Dataset[Row] = {
    val schemaMap = parseSchema(schema)
    val data = sc.textFile(path).map(x => parseLine(schemaMap._1, x.replace("\\xEF\\xBB\\xBF", ""), delim))
    sqlContext.createDataFrame(data, StructType(schemaMap._2))
  }

  /**
    * @input RDD[String]
    * @output Dataset[Row]
    */
  def toDf(sqlContext: SQLContext, schema: String, lines: Array[String], delim: String): Dataset[Row] = {
    val schemaMap = parseSchema(schema)
    val rows = lines.map { x => parseLine(schemaMap._1, x, delim) }.toList.asJava
    sqlContext.createDataFrame(rows, StructType(schemaMap._2))
  }

  /**
    * @input RDD[String]
    * @output Dataset[Row]
    */
  def toDf(sqlContext: SQLContext, schema: String, rdd: RDD[String], delim: String): Dataset[Row] = {
    val schemaMap = parseSchema(schema)
    val data = rdd.map { x => parseLine(schemaMap._1, x, delim) }
    sqlContext.createDataFrame(data, StructType(schemaMap._2))
  }

  // parse line to Row
  def parseLine(schema: Map[Int, String], data: String, delim: String): Row = {
    val line = data.split(delim)
    val row = ArrayBuffer.empty[Any]
    schema.toSeq.sorted.foreach {
      case (k, v) => {
        val value = Try {
          val d = Try { line(k) }.getOrElse("")
          v match {
            case "string"    => d.toString()
            case "text"      => d.toString()
            case "chararray" => d.toString()
            case "int"       => Try { d.toInt }.getOrElse(0)
            case "long"      => Try { d.toLong }.getOrElse(0l)
            case "double"    => Try { d.toDouble }.getOrElse(0d)
            case _           => 0
          }
        }
        row += value.get
      }
    }
    Row(row: _*)
  }

  // parse schema
  def parseSchema(schema: String): (Map[Int, String], Array[StructField]) = {
    val fieldMap = Map[Int, String]()
    var index = 0
    val fields = schema.split(",").map { x => x.trim().split(":") }.map {
      x =>
      {
        val dType = x(1).trim().toLowerCase()
        fieldMap += (index -> dType)
        index = index + 1
        StructField(x(0).trim(), dType match {
          case "string"    => StringType
          case "text"      => StringType
          case "chararray" => StringType
          case "int"       => IntegerType
          case "long"      => LongType
          case "double"    => DoubleType
          case _           => IntegerType
        }, nullable = true)
      }
    }
    (fieldMap, fields)
  }

  // read row
  def readRow(metadata: ResultSetMetaData, rs: ResultSet): Row = {
    val columnCount = metadata.getColumnCount()
    val row = ArrayBuffer.empty[Any]
    for(i <- 1 to columnCount) {
      val r = metadata.getColumnType(i) match {
        case Types.BIGINT => rs.getLong(i)
        case Types.INTEGER => rs.getInt(i)
        case Types.FLOAT => rs.getFloat(i)
        case Types.DOUBLE => rs.getDouble(i)
        case Types.DECIMAL => rs.getDouble(i)
        case _ => rs.getString(i)
      }
      row += r
    }
    Row(row: _*)
  }

  // parse sql metadata to spark schema
  def parseSchema(metadata: ResultSetMetaData): StructType = {
    val columnCount = metadata.getColumnCount()
    var schema = new Array[StructField](columnCount)
    for(i <- 1 to columnCount) {
      schema(i - 1) = StructField(metadata.getColumnName(i).trim(), metadata.getColumnType(i) match {
        case Types.INTEGER => IntegerType
        case Types.BIGINT => LongType
        case Types.FLOAT => DoubleType
        case Types.DOUBLE => DoubleType
        case Types.DECIMAL => DoubleType
        case _           => StringType
      }, nullable = true)
    }
    StructType(schema)
  }
}