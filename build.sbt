name := "dataplatform_utils"
organization := "com.goibibo"
version := "2.4"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
    "joda-time"         %   "joda-time"     % "2.10.1" % "provided",
    "org.slf4j"         %   "slf4j-api"     % "1.7.25" % "provided",
    "com.databricks"    %%  "spark-redshift" % "3.0.0-preview1" % "provided",
    "org.apache.zookeeper" %   "zookeeper" % "3.4.6" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.24" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided",
    "com.jsuereth" %% "scala-arm" % "2.0",
    ("org.joda" % "joda-convert" % "2.1.2").
        exclude("com.google.guava","guava")
)
