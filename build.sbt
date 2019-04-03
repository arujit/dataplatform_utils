name := "dataplatform_utils"
organization := "com.goibibo"
version := "2.2"
//Same as Spark 2.2's version ( https://github.com/apache/spark/blob/v2.2.0/pom.xml#L157 )
scalaVersion := "2.11.8"
resolvers += "spring_repo" at "http://repo.spring.io/plugins-release/"
resolvers += "central" at "http://central.maven.org/maven2/"
publishTo :=   Some("S3" at "s3://goibibo-libs/repo")

//publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
    "joda-time"         %   "joda-time"     % "2.9.7" % "provided",
    "org.slf4j"         %   "slf4j-api"     % "1.7.24" % "provided",
    "com.databricks"    %%  "spark-redshift" % "2.0.1" % "provided",
    "org.apache.zookeeper" %   "zookeeper" % "3.4.6" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.24" % "provided",
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0" % "provided",
    "com.jsuereth" %% "scala-arm" % "2.0",
    ("org.joda" % "joda-convert" % "1.8.2").
        exclude("com.google.guava","guava")
)