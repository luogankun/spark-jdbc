package com.luogankun.spark

import org.apache.spark.sql.SQLContext
import scala.collection.immutable.HashMap

/**
 * Created by spark on 15-1-6.
 */
package object jdbc {

  abstract class SchemaField extends  Serializable

  case class RegisteredSchemaField(fieldName:String, fieldType:String)
    extends SchemaField with Serializable

  case class JDBCSchemaField(fieldName:String, fieldType:String)
    extends  SchemaField with Serializable

  case class Parameter(name:String)

  protected val URL = Parameter("url")
  protected val USER = Parameter("user")
  protected val PASSWORD = Parameter("password")

  protected val SPARK_SQL_TABLE_SCHEMA = Parameter("sparksql_table_schema")
  protected val JDBC_TABLE_NAME = Parameter("jdbc_table_name")
  protected val JDBC_TABLE_SCHEMA = Parameter("jdbc_table_schema")

  protected val WHERE = Parameter("where")
  protected val NUMPARTITIONS = Parameter("num_partitions")


  implicit class JDBCContext(sqlContext:SQLContext) {
    def jdbcTable(url:String, user:String, password:String,
                  sparksqlTableSchema:String,jdbcTableName:String, jdbcTableSchama:String,
                  where:String, numPartitions:String) = {

      var params = new HashMap[String, String]
      params += (URL.name -> url)
      params += (USER.name -> user)
      params += (PASSWORD.name -> password)

      params += (SPARK_SQL_TABLE_SCHEMA.name -> sparksqlTableSchema)
      params += (JDBC_TABLE_NAME.name -> jdbcTableName)
      params += (JDBC_TABLE_SCHEMA.name -> jdbcTableSchama)

      params += (WHERE.name -> where)
      params += (NUMPARTITIONS.name -> numPartitions)

      sqlContext.baseRelationToSchemaRDD(JDBCRelation(params)(sqlContext))
    }
  }
}

