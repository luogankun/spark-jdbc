package com.luogankun.spark.jdbc

import org.apache.spark.sql.sources.{PrunedScan, RelationProvider}
import org.apache.spark.sql._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by spark on 15-1-21.
 */

class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    JDBCRelation(parameters)(sqlContext)
  }
}

/**
 * Created by spark on 15-1-19.
 */
case class JDBCRelation(@transient val jdbcProps: Map[String, String])(@transient val sqlContext: SQLContext) extends PrunedScan with Serializable {
  //jdbc
  val url = jdbcProps.getOrElse("url", sys.error("not valid url"))
  val user = jdbcProps.getOrElse("user", sys.error("not valid user"))
  val password = jdbcProps.getOrElse("password", sys.error("not valid password"))
  val sql = jdbcProps.getOrElse("sql", sys.error("not valid sql"))

  var jdbcTableFields = scala.collection.mutable.ArrayBuffer[JDBCSchemaField]()

  var dbFlag = 0
  //1:MySQL 2:ORACLE 3:DB2
  var driverName: String = "" //jdbc driver name

  if (url.matches( """^jdbc:mysql:.*""")) {
    driverName = "com.mysql.jdbc.Driver"
    dbFlag = 1
  } else if (url.matches( """^jdbc:db2:.*""")) {
    driverName = "com.ibm.db2.jcc.DB2Driver"
    dbFlag = 3
  } else if (url.matches( """^jdbc:oracle:thin:.*""")) {
    driverName = "oracle.jdbc.driver.OracleDriver"
    dbFlag = 2
  }

  def getSql() = {
    val tmp = sql
    println("~~~~~~~~~~~~~~dbFlag:~~~~~~~~~~" + dbFlag)
    val result = dbFlag match {
      case 1 => tmp + " LIMIT ?,?"
      case 2 => "SELECT * FROM (SELECT a.*, rownum as rnum FROM (" + tmp + ") a WHERE rownum <=? ) WHERE rnum >= ?"
      case 3 => "SELECT * FROM (SELECT t.*, row_number() over() rn from (" + tmp + ") t ) a1 WHERE a1.rn BETWEEN ? AND ?"
      case _ => "unsupported db driver"
    }
    result
  }

  lazy val schema = {
    var strutTypes = scala.collection.mutable.ArrayBuffer[StructField]()

    val stmt = getConnection().createStatement()
    val rs = stmt.executeQuery(sql)

    val rsmd = rs.getMetaData
    for (i <- 0 until rsmd.getColumnCount) {
      val fieldType = rsmd.getColumnTypeName(i + 1);
      val relatedType = fieldType.toLowerCase match {
        case "string" => SchemaType(StringType, nullable = false)
        case "varchar" => SchemaType(StringType, nullable = false)
        case "varchar2" => SchemaType(StringType, nullable = false)
        case "char" => SchemaType(StringType, nullable = false)
        case "int" => SchemaType(IntegerType, nullable = false)
        case "smallint" => SchemaType(IntegerType, nullable = false)
        case "long" => SchemaType(LongType, nullable = false)
        case "bigint" => SchemaType(LongType, nullable = false)
        case "number" => SchemaType(LongType, nullable = false)
        case "decimal" => SchemaType(DoubleType, nullable = false)
        case "timestamp" => SchemaType(TimestampType, nullable = false)
        case "date" => SchemaType(DateType, nullable = false)
        case "integer"=> SchemaType(IntegerType, nullable = false)
      }
      strutTypes += StructField(rsmd.getColumnLabel(i + 1), relatedType.dataType, relatedType.nullable)
      jdbcTableFields += JDBCSchemaField(rsmd.getColumnLabel(i + 1), fieldType)
    }

    StructType(strutTypes.toSeq)
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)

  override def buildScan(requiredColumns: Array[String]) = {
    //    for(ele <- requiredColumns) {
    //      println("columns ===>" + ele)
    //    }

    if(dbFlag == 2) {
      new JdbcRDD(sqlContext.sparkContext, getConnection, getSql(), Integer.MAX_VALUE, 0, 10, flatValue)
    }else{
      new JdbcRDD(sqlContext.sparkContext, getConnection, getSql(), 0, Integer.MAX_VALUE, 10, flatValue)
    }
//    new JdbcRDD(sqlContext.sparkContext, getConnection, sql + " LIMIT ?,?", 0, Integer.MAX_VALUE, 2, flatValue)
  }

  def flatValue(result: ResultSet) = {
    val values = new ArrayBuffer[Any]()
    jdbcTableFields.foreach({
      field => values += Resolver.resolve(field, result)
    })
    Row.fromSeq(values.toSeq)
  }

  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection(url, user, password)
  }
}


object Resolver extends Serializable {
  def resolve(jdbcField: JDBCSchemaField, rs: ResultSet): Any = {
    val fieldRs: Any = resolveColumn(rs, jdbcField.fieldName, jdbcField.fieldType)
    fieldRs
  }

  private def resolveColumn(rs: ResultSet, columnName: String, resultType: String): Any = {
    val column = resultType.toLowerCase match {
      case "string" => rs.getString(columnName)
      case "char" => rs.getString(columnName)
      case "varchar" => rs.getString(columnName)
      case "varchar2" => rs.getString(columnName)
      case "int" => rs.getInt(columnName)
      case "smallint" => rs.getInt(columnName)
      case "integer" => rs.getInt(columnName)
      case "bigint" => rs.getLong(columnName)
      case "number" => rs.getLong(columnName)
      case "decimal" => rs.getDouble(columnName)
      case "long" => rs.getLong(columnName)
      case "timestamp" => rs.getTimestamp(columnName)
      case "date" => rs.getDate(columnName)
    }
    column
  }
}