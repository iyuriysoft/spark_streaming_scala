//import AssemblyKeys._ 
Seq(assemblySettings: _*)

name := "stopfraud"
version := "0.1"
scalaVersion := "2.11.0"

//mainClass in (Compile, run) := Some("com.stopfraud.structured.StreamFile")
mainClass in (Compile, packageBin) := Some("com.stopfraud.structured.StreamKafka")
//retrieveManaged := true

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
                        "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0" % "provided",
                        "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
                        "org.apache.ignite" % "ignite-core" % "2.6.0",
                        "org.apache.ignite" % "ignite-spring" % "2.6.0" % "provided",
                        "org.apache.ignite" % "ignite-indexing" % "2.6.0",
                        "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
                        "org.slf4j" % "slf4j-api" % "1.6.4",
                        "org.slf4j" % "slf4j-log4j12" % "1.6.4")
                        