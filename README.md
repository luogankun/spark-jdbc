# Spark SQL JDBC Library

A library for parsing and querying CSV data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Requirements

This library requires Spark 1.2+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.luogankun.spark
artifactId: spark-jdbc_2.10
version: 0.1
```
The spark-jdbc assembly jar file can also be added to a Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-jdbc_2.10-0.1.jar
```

```sql
CREATE TEMPORARY TABLE jdbc_table
USING com.luogankun.spark.jdbc
OPTIONS (
  sparksql_table_schema  '(TBL_ID int, TBL_NAME string, TBL_TYPE string, DB_ID int)',
  jdbc_table_name    'TBLS',
  jdbc_table_schema '(TBL_ID , TBL_NAME , TBL_TYPE, DB_ID)',
  url    'jdbc:mysql://hadoop000:3306/hive',
  user    'root',
  password    'root',
  num_partitions  '5',
  where 'TBL_ID>8'
);

SELECT * FROM jdbc_table;
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt assembly` from the project root.

