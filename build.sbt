name := "kafka-beginners-course-scala"

version := "0.1"

scalaVersion := "2.13.8"

val kafkaScalaVersion = "3.1.0"
val ioCirceVersion = "0.14.2"
val slf4jVersion = "1.7.36"
val scalaLoggingVersion = "3.9.5"
val okHttp3Version = "4.10.0"
val okHttp3EventsourceVersion = "2.6.1"
val openSearchClientVersion = "2.0.0"
val gsonGoogleVersion = "2.9.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaScalaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaScalaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaScalaVersion,
  "io.circe" %% "circe-core" % ioCirceVersion,
  "io.circe" %% "circe-generic" % ioCirceVersion,
  "io.circe" %% "circe-parser" % ioCirceVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "com.squareup.okhttp3" % "okhttp" % okHttp3Version,
  "com.launchdarkly" % "okhttp-eventsource" % okHttp3EventsourceVersion,
  "org.opensearch.client" % "opensearch-rest-high-level-client" % openSearchClientVersion,
  "com.google.code.gson" % "gson" % gsonGoogleVersion
)