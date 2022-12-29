name := "final_project"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
 // https://mvnrepository.com/artifact/org.apache.spark/spark-core+
 "org.apache.spark" %% "spark-core" % "3.3.1",
 "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided" ,
 "org.apache.hadoop" % "hadoop-common" % "3.3.1",
 "org.apache.hadoop" % "hadoop-client" % "3.3.1" % "provided",
 "org.apache.hadoop" % "hadoop-hdfs" % "3.3.1" % Test,
// https://mvnrepository.com/artifact/org.postgresql/postgresql
  "org.postgresql" % "postgresql" % "42.5.1"
)

