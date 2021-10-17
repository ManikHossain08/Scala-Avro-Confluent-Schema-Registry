package bigdata.avro.schema.registry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import scala.collection.JavaConverters._

object AvroSchemaSubjects extends App {

  // 1. Instantiate a client of Schema Registry
  val schemaRegistryUrl = "http://localhost:8081"
  val srClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)

  // 2. Query schema registry
  val subjects: List[String] = srClient.getAllSubjects.asScala.toList

  // 3. retrieve the metadata of the first version (version is 1) "movie" subject and print it
  subjects foreach println

}
