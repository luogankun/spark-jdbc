# Spark SQL JDBC Library

A library for parsing and querying JDBC data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Requirements

This library requires Spark 1.2+

The spark-jdbc assembly jar file can also be added to a Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-sql --jars spark-jdbc_2.10-0.1.jar
```

```mysql
CREATE TEMPORARY TABLE jdbc_table
USING com.luogankun.spark.jdbc
OPTIONS (
url    'jdbc:mysql://hadoop000:3306/hive',
user    'root',
password    'root',
sql 'select TBL_ID,TBL_NAME,TBL_TYPE FROM TBLS'
);

SELECT * FROM jdbc_table;
```

```oracle
CREATE TEMPORARY TABLE jdbc_table
USING com.luogankun.spark.jdbc
OPTIONS (
url    'jdbc:oracle:thin:@hadoop000:1521/ora11g',
user    'coc',
password    'coc',
sql 'select HISTORY_ID, APPROVE_ROLE_ID, APPROVE_OPINION from CI_APPROVE_HISTORY'
);

SELECT * FROM jdbc_table;
```

```db2
CREATE TEMPORARY TABLE jdbc_table
USING com.luogankun.spark.jdbc
OPTIONS (
url    'jdbc:db2://hadoop000:60000/CI',
user    'ci',
password    'ci',
sql 'select LABEL_ID from coc.CI_APPROVE_STATUS'
);

SELECT * FROM jdbc_table;
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt assembly` from the project root.

##
Support Database: Mysql&Oracle&DB2
Support DataType: string&int&long&timestamp&date

