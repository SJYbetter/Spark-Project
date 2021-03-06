name := "HBaseJoin"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"
val hadoopVersion = "2.8.5"
val hbaseVersion = "1.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
