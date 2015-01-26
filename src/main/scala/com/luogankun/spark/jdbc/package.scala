package com.luogankun.spark

import org.apache.spark.sql.SQLContext
import scala.collection.immutable.HashMap

/**
 * Created by spark on 15-1-6.
 */
package object jdbc {

  abstract class SchemaField extends  Serializable

  case class JDBCSchemaField(fieldName:String, fieldType:String)
    extends  SchemaField with Serializable

  case class Parameter(name:String)

  protected val URL = Parameter("url")
  protected val USER = Parameter("user")
  protected val PASSWORD = Parameter("password")

  protected val SQL = Parameter("sql")

  implicit class JDBCContext(sqlContext:SQLContext) {
    def jdbcTable(url:String, user:String, password:String, sql:String) = {
      var params = new HashMap[String, String]
      params += (URL.name -> url)
      params += (USER.name -> user)
      params += (PASSWORD.name -> password)
      params += (SQL.name -> sql)

      sqlContext.baseRelationToSchemaRDD(JDBCRelation(params)(sqlContext))
    }
  }
}

