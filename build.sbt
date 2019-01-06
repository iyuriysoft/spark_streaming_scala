name := "stopfraud"
version := "0.1"
scalaVersion := "2.11.0"

mainClass in (Compile, run) := Some("com.structured.StreamFile")

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
                        "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
                        "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
                        "org.apache.ignite" % "ignite-core" % "2.6.0",
                        "org.apache.ignite" % "ignite-spring" % "2.6.0",
                        "org.apache.ignite" % "ignite-indexing" % "2.6.0",
                        "org.apache.logging.log4j" % "log4j-core" % "2.11.1")