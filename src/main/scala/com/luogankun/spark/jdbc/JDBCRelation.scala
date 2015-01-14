package com.luogankun.spark.jdbc

/**
 * Created by spark on 15-1-6.
 */

import org.apache.spark.sql.sources.TableScan
import scala.collection.mutable.ArrayBuffer
import java.sql.{ResultSet, DriverManager}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql._

/**
 * Created by spark on 15-1-4.
 */
case class JDBCRelation(@transient val jdbcProps:Map[String,String])(@transient val sqlContext:SQLContext) extends TableScan with Serializable{
  //jdbc
  val url = jdbcProps.getOrElse("url", sys.error("not valid url"))
  val user = jdbcProps.getOrElse("user", sys.error("not valid user"))
  val password = jdbcProps.getOrElse("password", sys.error("not valid password"))

  //configuration
  val jdbcTableName = jdbcProps.getOrElse("jdbc_table_name",
    sys.error("not valid schema"))
  val jdbcTableSchema = jdbcProps.getOrElse("jdbc_table_schema",
    sys.error("not valid jdbc_table_schema"))
  val registerTableSchame = jdbcProps.getOrElse("sparksql_table_schema",
    sys.error("not valid sparksql_table_schema"))

  val where = jdbcProps.getOrElse("where", "")
  val numPartitions = jdbcProps.getOrElse("num_partitions", "5").toInt

  //parse
  val tempJDBCFields = extractJdbcSchema(jdbcTableSchema)
  val registerTableFields = extractRegisterSchema(registerTableSchame)
  val tempFieldRelation = tableSchemaFieldMapping(tempJDBCFields,registerTableFields)
  val jdbcTableFields = feedTypes(tempFieldRelation)
  val fieldsRelations = tableSchemaFieldMapping(jdbcTableFields,registerTableFields)
  val queryColumns = getQueryTargetColumns(jdbcTableFields)

  var dbFlag = 0;  //1:mysql  2:oracle   3:db2
  var driverName:String = ""  //jdbc driver name

  if(url.matches("""^jdbc:mysql:.*""")) {
    driverName = "com.mysql.jdbc.Driver"
    dbFlag = 1
  } else if(url.matches("""^jdbc:db2:.*""")) {
    driverName = "com.ibm.db2.jcc.DB2Driver"
    dbFlag = 3
  } else if(url.matches("""^jdbc:oracle:thin:.*""")) {
    driverName = "oracle.jdbc.driver.OracleDriver"
    dbFlag = 2
  }


  lazy val schema = {
    val fields = jdbcTableFields.map({
      field => {
        val name = fieldsRelations.getOrElse(field,
          sys.error("table schema is not match the definition.")).fieldName
        val relatedType = field.fieldType match {
          case "string" => SchemaType(StringType, nullable = false)
          case "int" => SchemaType(IntegerType, nullable = false)
          case "long" => SchemaType(LongType, nullable = false)
          case "timestamp" => SchemaType(TimestampType, nullable = false)
          case "date" => SchemaType(DateType, nullable = false)
        }
        StructField(name,relatedType.dataType,relatedType.nullable)
      }
    })
    StructType(fields)
  }

   lazy val buildScan = {
      if(dbFlag == 2) {
        new JdbcRDD(sqlContext.sparkContext, getConnection, getSql(), Integer.MAX_VALUE, 0, numPartitions, flatValue)
      }else{
        new JdbcRDD(sqlContext.sparkContext, getConnection, getSql(), 0, Integer.MAX_VALUE, numPartitions, flatValue)
      }
  }

  private case class SchemaType(dataType:DataType, nullable:Boolean)

  def flatValue(result: ResultSet) = {
    val values = new ArrayBuffer[Any]()
    jdbcTableFields.foreach({
      field => values += Resolver.resolve(field,result)
    })
    Row.fromSeq(values.toSeq)
  }

  def extractJdbcSchema(jdbcTableSchema:String):Array[JDBCSchemaField] = {
    val fieldsStr = jdbcTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map(field => JDBCSchemaField(field,""))
  }

  def extractRegisterSchema(registerTableSchema:String):Array[RegisteredSchemaField] = {
    val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map({
      filed => {
        val split = filed.split("\\s+",-1)
        RegisteredSchemaField(split(0),split(1))
      }
    })
  }

  def tableSchemaFieldMapping(jdbcSchema:Array[JDBCSchemaField], registerSchema:Array[RegisteredSchemaField]):
    Map[JDBCSchemaField,RegisteredSchemaField] = {
    if(jdbcSchema.length != registerSchema.length) sys.error("column size not match in definition!")
    val rs = jdbcSchema.zip(registerSchema)
    rs.toMap
  }

  def feedTypes(mapping:Map[JDBCSchemaField,RegisteredSchemaField]):Array[JDBCSchemaField] = {
    val jdbcFields = mapping.map({
      case(k,v) => {
        val field = k.copy(fieldType = v.fieldType)
        field
      }
    })
    jdbcFields.toArray
  }

  def getQueryTargetColumns(jdbcTableFields:Array[JDBCSchemaField]):String = {
    var str = ArrayBuffer[String]()
    jdbcTableFields.foreach({
      field => {
        str += field.fieldName
      }
    })
    str.mkString(",")
  }

  def getSql() = {

    var whereCondition = ""
    if(where != ""){
      whereCondition += " WHERE " + where
    }

    val tmp = "select " + queryColumns + " FROM " + jdbcTableName +  whereCondition
    println("~~~~~~~~~~~~~~dbFlag:~~~~~~~~~~"+dbFlag)
    val result = dbFlag match {
      case 1 => tmp + " LIMIT ?,?"
      case 2 =>"SELECT * FROM (SELECT a.*, rownum as rnum FROM ("+tmp+") a WHERE rownum <=? ) WHERE rnum >= ?"
      case 3 =>  "SELECT * FROM (SELECT t.*, row_number() over() rn from ("+tmp+") t ) a1 WHERE a1.rn BETWEEN ? AND ?"
      case _ => "unsupported db driver"
    }
    //println("~~~~~~~~~~~driver : " + getDriverName(url) + " , dbFlag is: " + dbFlag + " , sql is: " + sql+ " , tmp is: " + tmp)
    println(result  + "!!!!!!!!!!!!!!!!!!!!!!!")
    result
  }

  //get database connection
  def getConnection() = {
    if(driverName == ""){
      sys.error("not supported driver.")
    } else {
      Class.forName(driverName).newInstance()
      DriverManager.getConnection(url, user, password)
    }
  }
}

object Resolver extends Serializable {
  def resolve(jdbcField:JDBCSchemaField, rs:ResultSet) : Any = {
    val fieldRs:Any = resolveColumn(rs, jdbcField.fieldName, jdbcField.fieldType)
    fieldRs
  }

  private def resolveColumn(rs:ResultSet, columnName:String, resultType:String):Any = {
    val column = resultType match {
      case "string" => rs.getString(columnName)
      case "int" => rs.getInt(columnName)
      case "long" => rs.getLong(columnName)
      case "timestamp" => rs.getTimestamp(columnName)
      case "date" => rs.getDate(columnName)
    }
    column
  }
}
