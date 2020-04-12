package org.pcg.walrus.common.io

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Dataset, SQLContext}
import java.io.Serializable

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.Try

import org.slf4j.{Logger, LoggerFactory}

/**
 * save data to jdbc
 * TODO support more data type(currently int|long|double|string is supported)
 */
object JdbcClient extends Serializable {
  
  private val log:Logger = LoggerFactory.getLogger(this.getClass)
  
  private def BATCH_SIZE = 1000

	/**
		* read mysql
		*/
	def readMysql(sqlContext: SQLContext, url: String, table: String, user: String, pwd: String,
								schema: String, filter: String="1=1", selectSql: String=""): Dataset[Row] = {
		val schemaMap = HdfsClient.parseSchema(schema)
		val fields = schemaMap._1
		val struct = StructType(schemaMap._2)
		log.info(s"fields: $fields, struct: $struct")

		// connection
		Class.forName("com.mysql.jdbc.Driver")
		val connection = DriverManager.getConnection(url, user, pwd)
		val statement = connection.createStatement()
		// for each result
		val cols = schema.split(",").map { x => x.split(":")(0).trim() }.mkString(",")
		val defaultSql = s"select $cols from $table where $filter"
		val sql = selectSql match {
			case a if a != null && a.length > 0 => selectSql
			case _ => defaultSql
		}
		log.info(s"read sql: $sql")
		val result = statement.executeQuery(sql)
		val rows = new java.util.ArrayList[Row]()
		while ( result.next() ) {
			rows.add(readRow(fields, result))
		}
		sqlContext.createDataFrame(rows, StructType(schemaMap._2))
	}

	// read row
	private def readRow(schema: Map[Int, String], result: ResultSet): Row = {
		val row = ArrayBuffer.empty[Any]
		schema.toSeq.sorted.foreach {
			case (k, v) => {
				val index = k + 1
				val value = Try {
					v match {
						case "string"    => result.getString(index)
						case "text"      => result.getString(index)
						case "chararray" => result.getString(index)
						case "int"       => result.getInt(index)
						case "long"      => result.getLong(index)
						case "double"    => result.getDouble(index)
						case _           => 0
					}
				}
				row += value.get
			}
		}
		Row(row: _*)
	}

	/**
    * save to mysql
   */
  def saveToMysql(data: Dataset[Row], url: String, table: String, mode: String,
									partitonNum: Int = 10) = {
    //clear table if necessary
    mode match {
      case "overwrite" => {
        val sql = s"DELETE FROM $table"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url)
        val stmt = conn.createStatement()
        stmt.executeUpdate(sql)
      }
      case _ =>
    }
    implicit val rddEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    val schema = data.schema.fields.map { f => f.name }
    val types = data.schema.fields.map { f => f.dataType.typeName }
    log.info(s"mysql save schema: ${schema.mkString(",")}[${types.mkString(",")}]")
    val result = data.repartition(partitonNum).mapPartitions { rows => {
    	  Class.forName("com.mysql.jdbc.Driver")
    		val conn = DriverManager.getConnection(url)
    		val asks = schema.map { x => "?" }
    		val sql = s"INSERT INTO $table (${schema.mkString(",")}) VALUES(${asks.mkString(",")})"
    		log.info(s"JdbcSaver insert sql: $sql")
    		val pstmt = conn.prepareStatement(sql)
    		var rowCount = 0
    		var rs = ListBuffer[Row]()
    	  while(rows.hasNext) {
    			val row = rows.next()
    			rs += row
    			row.schema.fields.map { x => {
    				val index = row.fieldIndex(x.name)
    				x.dataType.typeName.toLowerCase() match {
    					case "integer" => pstmt.setInt(index + 1, Try { row.getInt(index) }.getOrElse(0))
    					case "long" => pstmt.setLong(index + 1, Try { row.getLong(index) }.getOrElse(0))
    					case "double" => pstmt.setDouble(index + 1, Try { row.getDouble(index) }.getOrElse(0))
    					case _ => pstmt.setString(index + 1, Try { row.getString(index) }.getOrElse(""))
  					}
    		  }}
    			pstmt.addBatch()
    			rowCount += 1
    			if (rowCount % BATCH_SIZE == 0) {
  				 pstmt.executeBatch()
  				 rowCount = 0
  			  }
    		}
    		if (rowCount > 0) pstmt.executeBatch()
  		  rs.iterator
    }}
    // trigger action
    result.count()
   }
}