name := "dataplatform_utils"
organization := "com.goibibo"
version := "1.6"
scalaVersion := "2.10.6"

resolvers += "Cloudera Repo for Spark-HBase" at "https://repository.cloudera.com/content/repositories/releases/"
resolvers += "Hortonworks Repo" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
    "org.apache.hbase" % "hbase" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided",
    "joda-time" % "joda-time" % "2.9.7" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.24" % "provided",
    "com.jsuereth" %% "scala-arm" % "2.0",
    "org.apache.kafka" % "kafka-clients" % "0.9.0.2.4.2.12-1" % "provided",
    "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.8.0" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0-cdh5.8.2" % "provided",
    "org.apache.kafka" %% "kafka" % "0.9.0.0" % "provided"
)
