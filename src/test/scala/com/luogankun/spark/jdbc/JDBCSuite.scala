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
//    val results = TestSQLContext.jdbcTable("jdbc:mysql://hadoop000:3306/hive","root","root","(TBL_ID int, TBL_NAME string, TBL_TYPE string)","TBLS", "(TBL_ID , TBL_NAME , TBL_TYPE)").select('TBL_NAME).collect()
//    assert(results.size === 4)
  }

  test("sql test") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE jdbc_table
        |USING com.luogankun.spark.jdbc
        |OPTIONS (
        |  sparksql_table_schema  '(TBL_ID int, TBL_NAME string, TBL_TYPE string, DB_ID int)',
        |  jdbc_table_name    'TBLS',
        |  jdbc_table_schema '(TBL_ID , TBL_NAME , TBL_TYPE, DB_ID)',
        |  url    'jdbc:mysql://hadoop000:3306/hive',
        |  user    'root',
        |  password    'root',
        |  num_partitions  '5',
        |  where 'TBL_ID>8'
        |)""".stripMargin)

    sql("SELECT * FROM jdbc_table").collect.foreach(println)

    assert(sql("SELECT * FROM jdbc_table").collect().size == 2 )
  }
}
