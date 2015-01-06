package com.luogankun.spark.jdbc

import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.SQLContext

/**
 * Created by spark on 15-1-6.
 */
class DefaultSource extends RelationProvider {
  def createRelation(sqlContext:SQLContext, parameters:Map[String,String]) = {
    JDBCRelation(parameters)(sqlContext)
  }
}
