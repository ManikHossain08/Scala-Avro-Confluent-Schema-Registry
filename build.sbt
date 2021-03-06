name := "Scala-Avro-Confluent-Schema-Registry"

version := "0.1"

scalaVersion := "2.12.12"

val ConfluentVersion = "5.5.1"

resolvers += "Confluent".at("https://packages.confluent.io/maven/")

libraryDependencies += "org.apache.kafka" % "kafka-clients"                % "2.5.1"
libraryDependencies += "io.confluent"     % "kafka-schema-registry-client" % ConfluentVersion
libraryDependencies += "io.confluent"     % "kafka-avro-serializer"        % ConfluentVersion