arpityadav@arpityadav:~$ spark-shell
2025-09-25 09:19:16 WARN  Utils:66 - Your hostname, arpityadav resolves to a loopback address: 127.0.1.1; using 192.168.64.5 instead (on interface enp0s1)
2025-09-25 09:19:16 WARN  Utils:66 - Set SPARK_LOCAL_IP if you need to bind to another address
2025-09-25 09:19:16 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.64.5:4040
Spark context available as 'sc' (master = local[*], app id = local-1758791961373).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/
         
Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_462)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder().master("local[1]").appName("SparkExample").getOrCreate()
2025-10-06 08:07:10 WARN  SparkSession$Builder:66 - Using an existing SparkSession; some configuration may not take effect.
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@5477d90e

scala> val df = spark.read.option("header",true).csv("         ")
org.apache.spark.sql.AnalysisException: Path does not exist: file:/home/arpityadav/         ;
  at org.apache.spark.sql.execution.datasources.DataSource$.org$apache$spark$sql$execution$datasources$DataSource$$checkAndGlobPathIfNecessary(DataSource.scala:715)
  at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$15.apply(DataSource.scala:389)
  at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$15.apply(DataSource.scala:389)
  at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
  at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at scala.collection.TraversableLike$class.flatMap(TraversableLike.scala:241)
  at scala.collection.immutable.List.flatMap(List.scala:344)
  at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:388)
  at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:239)
  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:227)
  at org.apache.spark.sql.DataFrameReader.csv(DataFrameReader.scala:596)
  at org.apache.spark.sql.DataFrameReader.csv(DataFrameReader.scala:473)
  ... 49 elided

scala> Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_462)
<console>:1: error: ';' expected but double literal found.
Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_462)
                    ^
<console>:1: error: Invalid literal number
Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_462)
                                                              ^

scala> Type in expressions to have them evaluated.
     | Type :help for more information.
<console>:2: error: ';' expected but 'for' found.
Type :help for more information.
           ^

scala> 

scala> scala> import org.apache.spark.sql.SparkSession

// Detected repl transcript. Paste more, or ctrl-D to finish.

import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder().master("local[1]").appName("SparkExample").getOrCreate()
2025-10-06 08:07:10 WARN  SparkSession$Builder:66 - Using an existing SparkSession; some configuration may not take effect.
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@5477d90e



arpityadav@arpityadav:~$ ^C
arpityadav@arpityadav:~$ spark-shell
2025-10-06 08:13:13 WARN  Utils:66 - Your hostname, arpityadav resolves to a loopback address: 127.0.1.1; using 192.168.64.5 instead (on interface enp0s1)
2025-10-06 08:13:13 WARN  Utils:66 - Set SPARK_LOCAL_IP if you need to bind to another address
2025-10-06 08:13:14 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.64.5:4040
Spark context available as 'sc' (master = local[*], app id = local-1759738397465).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/
         
Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_462)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> 

scala> val spark = SparkSession.builder()
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@134d7ffa

scala>   .master("local[1]")
<console>:1: error: illegal start of definition
  .master("local[1]")
  ^

scala>   .appName("SparkExample")
<console>:1: error: illegal start of definition
  .appName("SparkExample")
  ^

scala>   .getOrCreate()
<console>:1: error: illegal start of definition
  .getOrCreate()
  ^

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder().master("local[1]").appName("SparkExample").getOrCreate()
2025-10-06 08:14:33 WARN  SparkSession$Builder:66 - Using an existing SparkSession; some configuration may not take effect.
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@158e6fc2

scala> val df = spark.read.option("header",true).csv("/home/arpityadav/Desktop/data.csv")
2025-10-06 08:16:07 WARN  ObjectStore:6666 - Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
2025-10-06 08:16:07 WARN  ObjectStore:568 - Failed to get database default, returning NoSuchObjectException
2025-10-06 08:16:07 WARN  ObjectStore:568 - Failed to get database global_temp, returning NoSuchObjectException
df: org.apache.spark.sql.DataFrame = [Name: string, Age: string ... 2 more fields]

scala> spark.sql("select * from data").show()
org.apache.spark.sql.AnalysisException: Table or view not found: data; line 1 pos 14
  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:47)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveRelations$$lookupTableFromCatalog(Analyzer.scala:665)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.resolveRelation(Analyzer.scala:617)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$$anonfun$apply$8.applyOrElse(Analyzer.scala:647)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$$anonfun$apply$8.applyOrElse(Analyzer.scala:640)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:306)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:304)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.apply(Analyzer.scala:640)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.apply(Analyzer.scala:586)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1$$anonfun$apply$1.apply(RuleExecutor.scala:87)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1$$anonfun$apply$1.apply(RuleExecutor.scala:84)
  at scala.collection.LinearSeqOptimized$class.foldLeft(LinearSeqOptimized.scala:124)
  at scala.collection.immutable.List.foldLeft(List.scala:84)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1.apply(RuleExecutor.scala:84)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1.apply(RuleExecutor.scala:76)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:76)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.org$apache$spark$sql$catalyst$analysis$Analyzer$$executeSameContext(Analyzer.scala:124)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.execute(Analyzer.scala:118)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:103)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:74)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided
Caused by: org.apache.spark.sql.catalyst.analysis.NoSuchTableException: Table or view 'data' not found in database 'default';
  at org.apache.spark.sql.hive.client.HiveClient$$anonfun$getTable$1.apply(HiveClient.scala:81)
  at org.apache.spark.sql.hive.client.HiveClient$$anonfun$getTable$1.apply(HiveClient.scala:81)
  at scala.Option.getOrElse(Option.scala:121)
  at org.apache.spark.sql.hive.client.HiveClient$class.getTable(HiveClient.scala:81)
  at org.apache.spark.sql.hive.client.HiveClientImpl.getTable(HiveClientImpl.scala:83)
  at org.apache.spark.sql.hive.HiveExternalCatalog$$anonfun$getRawTable$1.apply(HiveExternalCatalog.scala:118)
  at org.apache.spark.sql.hive.HiveExternalCatalog$$anonfun$getRawTable$1.apply(HiveExternalCatalog.scala:118)
  at org.apache.spark.sql.hive.HiveExternalCatalog.withClient(HiveExternalCatalog.scala:97)
  at org.apache.spark.sql.hive.HiveExternalCatalog.getRawTable(HiveExternalCatalog.scala:117)
  at org.apache.spark.sql.hive.HiveExternalCatalog$$anonfun$getTable$1.apply(HiveExternalCatalog.scala:684)
  at org.apache.spark.sql.hive.HiveExternalCatalog$$anonfun$getTable$1.apply(HiveExternalCatalog.scala:684)
  at org.apache.spark.sql.hive.HiveExternalCatalog.withClient(HiveExternalCatalog.scala:97)
  at org.apache.spark.sql.hive.HiveExternalCatalog.getTable(HiveExternalCatalog.scala:683)
  at org.apache.spark.sql.catalyst.catalog.SessionCatalog.lookupRelation(SessionCatalog.scala:669)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveRelations$$lookupTableFromCatalog(Analyzer.scala:662)
  ... 81 more

scala> df.createOrReplaceTempView("data_table")  // "data_table" is the SQL table name

scala> spark.sql("SELECT * FROM data_table").show()
+-----+---+----------+------+
| Name|Age|Department|Salary|
+-----+---+----------+------+
| John| 30|        IT| 50000|
|Alice| 35|        HR| 60000|
|  Bob| 40|   Finance| 70000|
+-----+---+----------+------+


scala> spark.sql("select Department, avg(age) from data_table").show()
org.apache.spark.sql.AnalysisException: grouping expressions sequence is empty, and 'data_table.`Department`' is not an aggregate function. Wrap '(avg(CAST(data_table.`age` AS DOUBLE)) AS `avg(CAST(age AS DOUBLE))`)' in windowing function(s) or wrap 'data_table.`Department`' in first() (or first_value) if you don't care which value you get.;;
Aggregate [Department#12, avg(cast(age#11 as double)) AS avg(CAST(age AS DOUBLE))#41]
+- SubqueryAlias data_table
   +- Relation[Name#10,Age#11,Department#12,Salary#13] csv

  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.failAnalysis(CheckAnalysis.scala:41)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.failAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.org$apache$spark$sql$catalyst$analysis$CheckAnalysis$class$$anonfun$$checkValidAggregateExpression$1(CheckAnalysis.scala:179)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$9.apply(CheckAnalysis.scala:220)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$9.apply(CheckAnalysis.scala:220)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:220)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:105)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:74)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided

scala> spark.sql("select Department, avg(age) from data_table groupby Department").show()
org.apache.spark.sql.catalyst.parser.ParseException:
extraneous input 'Department' expecting <EOF>(line 1, pos 52)

== SQL ==
select Department, avg(age) from data_table groupby Department
----------------------------------------------------^^^

  at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:239)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:115)
  at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:48)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:69)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided

scala> spark.sql("select Department, avg(age) from data_table group by Department").show()
+----------+------------------------+
|Department|avg(CAST(age AS DOUBLE))|
+----------+------------------------+
|        HR|                    35.0|
|   Finance|                    40.0|
|        IT|                    30.0|
+----------+------------------------+


scala> val df = spark.read.option("header",true).csv("/home/arpityadav/Desktop/data.csv")
df: org.apache.spark.sql.DataFrame = [Transaction_ID: string, Product: string ... 1 more field]

scala> df.createOrReplaceTempView("data_table")  // "data_table" is the SQL table name

scala> spark.sql("SELECT * FROM data_table").show()
+--------------+-------+------+
|Transaction_ID|Product|Amount|
+--------------+-------+------+
|             1| Laptop|  1000|
|             2|  Phone|   500|
|             3| Laptop|  1200|
|             4|     TV|   800|
+--------------+-------+------+


scala> parl.sql(select Product, sum(Amount) from data_table where Product = 'Laptop')
<console>:1: error: unclosed character literal
parl.sql(select Product, sum(Amount) from data_table where Product = 'Laptop')
                                                                            ^

scala> parl.sql(select Product, sum(Amount) from data_table where Product = 'Laptop').show()
<console>:1: error: unclosed character literal
parl.sql(select Product, sum(Amount) from data_table where Product = 'Laptop').show()
                                                                            ^

scala> spark.sql(select Product, sum(Amount) from data_table where Product = 'Laptop').show()
<console>:1: error: unclosed character literal
spark.sql(select Product, sum(Amount) from data_table where Product = 'Laptop').show()
                                                                             ^

scala> spark.sql(select Product, sum(Amount) from data_table where Product = Laptop).show()
<console>:29: error: too many arguments for method sql: (sqlText: String)org.apache.spark.sql.DataFrame
       spark.sql(select Product, sum(Amount) from data_table where Product = Laptop).show()
                ^

scala> spark.sql(select Product, sum(Amount) from data_table where Product =" Laptop").show()
<console>:29: error: too many arguments for method sql: (sqlText: String)org.apache.spark.sql.DataFrame
       spark.sql(select Product, sum(Amount) from data_table where Product =" Laptop").show()
                ^

scala> spark.sql("select Product, sum(Amount) from data_table where Product ='Laptop'").show()
org.apache.spark.sql.AnalysisException: grouping expressions sequence is empty, and 'data_table.`Product`' is not an aggregate function. Wrap '(sum(CAST(data_table.`Amount` AS DOUBLE)) AS `sum(CAST(Amount AS DOUBLE))`)' in windowing function(s) or wrap 'data_table.`Product`' in first() (or first_value) if you don't care which value you get.;;
Aggregate [Product#71, sum(cast(Amount#72 as double)) AS sum(CAST(Amount AS DOUBLE))#94]
+- Filter (Product#71 = Laptop)
   +- SubqueryAlias data_table
      +- Relation[Transaction_ID#70,Product#71,Amount#72] csv

  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.failAnalysis(CheckAnalysis.scala:41)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.failAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.org$apache$spark$sql$catalyst$analysis$CheckAnalysis$class$$anonfun$$checkValidAggregateExpression$1(CheckAnalysis.scala:179)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$9.apply(CheckAnalysis.scala:220)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$9.apply(CheckAnalysis.scala:220)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:220)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:105)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:74)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided

scala> spark.sql(
     |   "SELECT Product, SUM(CAST(Amount AS DOUBLE)) AS total_amount " +
     |   "FROM data_table " +
     |   "WHERE Product = 'Laptop' " +
     |   "GROUP BY Product"
     | ).show()
+-------+------------+
|Product|total_amount|
+-------+------------+
| Laptop|      2200.0|
+-------+------------+


scala> val emp1 = sc.parallelize(List((10,"Inventory","Hybd"),(20,"Finance",bglr),(30,"HR","Mumbai"),(40,"Admin","che"))).toDF("DeptNo","Dname","Locccval emp1 = sc.parallelize(List(")
<console>:27: error: not found: value bglr
       val emp1 = sc.parallelize(List((10,"Inventory","Hybd"),(20,"Finance",bglr),(30,"HR","Mumbai"),(40,"Admin","che"))).toDF("DeptNo","Dname","Locccval emp1 = sc.parallelize(List(")
                                                                            ^

scala>   (10, "Inventory", "Hybd"),
<console>:1: error: ';' expected but ',' found.
  (10, "Inventory", "Hybd"),
                           ^

scala>   (20, "Finance", "Bglr"),
<console>:1: error: ';' expected but ',' found.
  (20, "Finance", "Bglr"),
                         ^

scala>   (30, "HR", "Mumbai"),
<console>:1: error: ';' expected but ',' found.
  (30, "HR", "Mumbai"),
                      ^

scala>   (40, "Admin", "Che")
res12: (Int, String, String) = (40,Admin,Che)

scala> )).toDF("DeptNo", "Dname", "Loc")
<console>:1: error: illegal start of definition
)).toDF("DeptNo", "Dname", "Loc")
^

scala> val emp1 = sc.parallelize(List(
     |   (10, "Inventory", "Hybd"),
     |   (20, "Finance", "Bglr"),
     |   (30, "HR", "Mumbai"),
     |   (40, "Admin", "Che")
     | )).toDF("DeptNo", "Dname", "Loc")
emp1: org.apache.spark.sql.DataFrame = [DeptNo: int, Dname: string ... 1 more field]

scala> emp1.show()
+------+---------+------+
|DeptNo|    Dname|   Loc|
+------+---------+------+
|    10|Inventory|  Hybd|
|    20|  Finance|  Bglr|
|    30|       HR|Mumbai|
|    40|    Admin|   Che|
+------+---------+------+


scala> val emp2 = sc.parallelize(List(
     |   (10, "Alice", 5000.0),
     |   (20, "Bob", 6000.0),
     |   (30, "Charlie", 5500.0),
     |   (50, "David", 7000.0) // DeptNo 50 does not exist in emp1
     | )).toDF("DeptNo", "EmpName", "Salary")
emp2: org.apache.spark.sql.DataFrame = [DeptNo: int, EmpName: string ... 1 more field]

scala> emp2.show()
+------+-------+------+
|DeptNo|EmpName|Salary|
+------+-------+------+
|    10|  Alice|5000.0|
|    20|    Bob|6000.0|
|    30|Charlie|5500.0|
|    50|  David|7000.0|
+------+-------+------+


scala> emp1.createOrReplaceTempView("emp1_table")

scala> emp2.createOrReplaceTempView("emp1_table")

scala> emp2.createOrReplaceTempView("emp2_table")

scala> spark.sql("""""")
org.apache.spark.sql.catalyst.parser.ParseException:
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 1, pos 0)

== SQL ==

^^^

  at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:239)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:115)
  at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:48)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:69)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided

scala> spark.sql("""""")
org.apache.spark.sql.catalyst.parser.ParseException:
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 1, pos 0)

== SQL ==

^^^

  at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:239)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:115)
  at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:48)
  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:69)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 50 elided

scala> spark.sql(
     |   """
     |     |SELECT e1.DeptNo, e1.Dname, e1.Loc, e2.EmpName, e2.Salary
     |     |FROM emp1_table e1
     |     |INNER JOIN emp2_table e2
     |     |ON e1.DeptNo = e2.DeptNo
     |   """.stripMargin
     | ).show()
org.apache.spark.sql.AnalysisException: cannot resolve '`e1.Dname`' given input columns: [e2.Salary, e2.DeptNo, e2.EmpName, e1.Salary, e1.DeptNo, e1.EmpName]; line 2 pos 18;
'Project [DeptNo#144, 'e1.Dname, 'e1.Loc, EmpName#162, Salary#163]
+- Join Inner, (DeptNo#144 = DeptNo#161)
   :- SubqueryAlias e1
   :  +- SubqueryAlias emp1_table
   :     +- Project [_1#140 AS DeptNo#144, _2#141 AS EmpName#145, _3#142 AS Salary#146]
   :        +- SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#140, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2, true, false) AS _2#141, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3 AS _3#142]
   :           +- ExternalRDD [obj#139]
   +- SubqueryAlias e2
      +- SubqueryAlias emp2_table
         +- Project [_1#140 AS DeptNo#161, _2#141 AS EmpName#162, _3#142 AS Salary#163]
            +- SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#140, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2, true, false) AS _2#141, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3 AS _3#142]
               +- ExternalRDD [obj#139]

  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:88)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:107)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:107)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:106)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:118)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1$1.apply(QueryPlan.scala:122)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.immutable.List.map(List.scala:285)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:122)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$2.apply(QueryPlan.scala:127)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:127)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:105)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:74)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 57 elided

scala> emp1.createOrReplaceTempView("emp1_table")

scala> emp2.createOrReplaceTempView("emp2_table")

scala> 

scala> spark.sql(
     |   """
     |     |SELECT e1.DeptNo, e1.EmpName AS DeptEmpName, e1.Salary AS DeptSalary,
     |     |       e2.EmpName AS EmpName, e2.Salary AS EmpSalary
     |     |FROM emp1_table e1
     |     |INNER JOIN emp2_table e2
     |     |ON e1.DeptNo = e2.DeptNo
     |   """.stripMargin
     | ).show()
org.apache.spark.sql.AnalysisException: cannot resolve '`e1.EmpName`' given input columns: [e2.Salary, e2.DeptNo, e1.Dname, e1.DeptNo, e1.Loc, e2.EmpName]; line 2 pos 18;
'Project [DeptNo#119, 'e1.EmpName AS DeptEmpName#164, 'e1.Salary AS DeptSalary#165, EmpName#145 AS EmpName#166, Salary#146 AS EmpSalary#167]
+- Join Inner, (DeptNo#119 = DeptNo#144)
   :- SubqueryAlias e1
   :  +- SubqueryAlias emp1_table
   :     +- Project [_1#115 AS DeptNo#119, _2#116 AS Dname#120, _3#117 AS Loc#121]
   :        +- SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#115, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2, true, false) AS _2#116, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3, true, false) AS _3#117]
   :           +- ExternalRDD [obj#114]
   +- SubqueryAlias e2
      +- SubqueryAlias emp2_table
         +- Project [_1#140 AS DeptNo#144, _2#141 AS EmpName#145, _3#142 AS Salary#146]
            +- SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#140, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2, true, false) AS _2#141, assertnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3 AS _3#142]
               +- ExternalRDD [obj#139]

  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:88)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:306)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:304)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:286)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:107)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:107)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:106)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:118)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1$1.apply(QueryPlan.scala:122)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.immutable.List.map(List.scala:285)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:122)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$2.apply(QueryPlan.scala:127)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:127)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:95)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:80)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:92)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:105)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:74)
  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:641)
  ... 58 elided

scala> emp1.createOrReplaceTempView("emp1_table")

scala> emp2.createOrReplaceTempView("emp2_table")

scala> spark.sql(
     |   """
     |     |SELECT e1.DeptNo, e1.Dname, e1.Loc, e2.EmpName, e2.Salary
     |     |FROM emp1_table e1
     |     |INNER JOIN emp2_table e2
     |     |ON e1.DeptNo = e2.DeptNo
     |   """.stripMargin
     | ).show()
+------+---------+------+-------+------+
|DeptNo|    Dname|   Loc|EmpName|Salary|
+------+---------+------+-------+------+
|    20|  Finance|  Bglr|    Bob|6000.0|
|    10|Inventory|  Hybd|  Alice|5000.0|
|    30|       HR|Mumbai|Charlie|5500.0|
+------+---------+------+-------+------+


scala> val tb1 = sc.parallelize(List((1,"Alice",101,600000),(2,"Bob",102,55000),(3,"Charlie",101,62000),(4,"David",103,58000))).toDF("employee_id","Name","Dept_ID","Salary")
tb1: org.apache.spark.sql.DataFrame = [employee_id: int, Name: string ... 2 more fields]

scala> tb1.show()
+-----------+-------+-------+------+
|employee_id|   Name|Dept_ID|Salary|
+-----------+-------+-------+------+
|          1|  Alice|    101|600000|
|          2|    Bob|    102| 55000|
|          3|Charlie|    101| 62000|
|          4|  David|    103| 58000|
+-----------+-------+-------+------+


scala> tb1.createOrReplaceTempView("Table1")

scala> 
