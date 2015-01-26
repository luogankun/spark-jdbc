package com.luogankun.spark.jdbc

/**
 * Created by spark on 15-1-6.
 */

import org.scalatest.FunSuite
import org.apache.spark.sql.test.TestSQLContext

/* Implicits */
import TestSQLContext._

/**
 * Created by spark on 15-1-6.
 */
class JDBCSuite extends FunSuite{

  test("dsl test") {
    val sql = "select a.TBL_ID, a.TBL_NAME, b.DB_ID, b.DB_LOCATION_URI from TBLS a join DBS b on a.DB_ID = b.DB_ID"
    val results = TestSQLContext.jdbcTable("jdbc:mysql://hadoop000:3306/hive","root","root",sql)
    results.registerTempTable("a")
    TestSQLContext.sql("select * from a").collect.foreach(println)


  }


  test("abc") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE spark_tbls
        |USING com.luogankun.spark.jdbc
        |OPTIONS (
        |  url 'jdbc:mysql://hadoop000:3306/test',
        |  user 'root',
        |  password 'root',
        |  sql "select id, name from city"
        |)""".stripMargin)

    sql("SELECT id, name FROM spark_tbls").collect.foreach(println)
  }

}
