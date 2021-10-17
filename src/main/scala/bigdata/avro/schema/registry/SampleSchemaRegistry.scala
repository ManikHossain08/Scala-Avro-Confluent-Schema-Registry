package bigdata.avro.schema.registry

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import org.apache.avro.{Schema, SchemaBuilder}

import java.io.InputStream
import scala.collection.JavaConverters._
import scala.io.Source

object SampleSchemaRegistry extends App {
  // 1. Schema to register
  // 1.a have it in a file and read it in your application
  val movieSchemaFromFileSource: InputStream = getClass.getResourceAsStream("/movie.avsc")
  val movieSchemaFromFileStr: String = Source.fromInputStream(movieSchemaFromFileSource).getLines().mkString("\n")
  val movieSchemaFromFile: Schema = new Schema.Parser().parse(movieSchemaFromFileStr)

  // 1.b create schema programmatically
  val movieSchemaProgrammatically: Schema =
    SchemaBuilder
      .record("Movie")
      .namespace("ca.mcit.bigdata.avro")
      .fields()
      .requiredInt("mId")
      .requiredString("title")
      .requiredInt("year")
      .name("director")
      .`type`()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .endUnion()
      .nullDefault()
      .endRecord()

  // 1.c define the schema in Avro IDL (easier to maintain) and use tools to convert to Avro schema

  // 2. connect to the schema registry
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)

  // 3. register the schema
  val id = srClient.register("Movie", new AvroSchema(movieSchemaProgrammatically).asInstanceOf[ParsedSchema])
  println(s"Registered 'movie' schema with ID $id")

  // 2- Retrieve schema (most of the time, we use this method)
  // 2.a- retrieve the ID of the latest version of the schema of the subject
  val movieSubjectMetadata: SchemaMetadata = srClient.getLatestSchemaMetadata("Movie")
  println(s"Version: ${movieSubjectMetadata.getVersion}")
  println(s"ID: ${movieSubjectMetadata.getId}")
  // 2.b- retrieve the schema by ID which is a unique number
  val movieSchema: ParsedSchema = srClient.getSchemaById(movieSubjectMetadata.getId)

  // 3- use the schema
  println(movieSchema)
  srClient.getAllSubjectsById(562).asScala.toList foreach println // get Subject name


  /**
   * Exercise: register the schema for 'rating'
   *
   * case class Rating(rID: Int, mID: Int, stars: Int, ratingDate: Option[String])
   */
}
