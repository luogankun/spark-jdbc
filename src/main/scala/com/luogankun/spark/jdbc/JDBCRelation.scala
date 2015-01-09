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

  val where = jdbcProps.getOrElse("where", sys.error("not valid where"))
  val numPartitions = jdbcProps.getOrElse("num_partitions", sys.error("not valid num_partitions")).toInt

  //parse
  val tempJDBCFields = extractJdbcSchema(jdbcTableSchema)
  val registerTableFields = extractRegisterSchema(registerTableSchame)
  val tempFieldRelation = tableSchemaFieldMapping(tempJDBCFields,registerTableFields)
  val jdbcTableFields = feedTypes(tempFieldRelation)
  val fieldsRelations = tableSchemaFieldMapping(jdbcTableFields,registerTableFields)
  val queryColumns = getQueryTargetColumns(jdbcTableFields)


  lazy val schema = {
    val fields = jdbcTableFields.map({
      field => {
        val name = fieldsRelations.getOrElse(field,
          sys.error("table schema is not match the definition.")).fieldName
        val relatedType = field.fieldType match {
          case "string" => SchemaType(StringType, nullable = false)
          case "int" => SchemaType(IntegerType, nullable = false)
          case "long" => SchemaType(LongType, nullable = false)
        }
        StructField(name,relatedType.dataType,relatedType.nullable)
      }
    })
    StructType(fields)
  }

  lazy val buildScan = {
    val sql = "select " + queryColumns + " from " + jdbcTableName + " where " + "TBL_ID >= ? and TBL_ID <= ? AND " + where
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + sql)
    val baseRDD = new JdbcRDD(sqlContext.sparkContext, getConnection, sql, 1, 10, numPartitions, flatValue)
    baseRDD
  }

  private case class SchemaType(dataType:DataType, nullable:Boolean)

  //get database connection
  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection(url, user, password)
  }

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
    }
    column
  }
}

