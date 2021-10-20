package bigdata.avro.schema.registry

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.InputStream
import java.util.Properties
import scala.io.Source

object ProduceAvroGenericRecord extends App {
  val movieSubjectName = "enriched_trip"

  // 1. prepare data
  val enrichedTripCsv = getClass.getResourceAsStream("/enriched_trips.csv")
  val enrichedTrips: List[String] = Source.fromInputStream(enrichedTripCsv).getLines().toList.tail

  // 1.a have it in a file and read it in your application
  val tripSchemaFromFileSource: InputStream = getClass.getResourceAsStream("/enriched_trip.avsc")
  val tripSchemaFromFileStr: String = Source.fromInputStream(tripSchemaFromFileSource).getLines().mkString("\n")
  val movieSchemaFromFile: Schema = new Schema.Parser().parse(tripSchemaFromFileStr)


  // 2. retrieve the Avro schema from Schema Registry
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)
  val id = srClient.register(movieSubjectName, new AvroSchema(movieSchemaFromFile).asInstanceOf[ParsedSchema])

  val enrichedTripMetadata = srClient.getLatestSchemaMetadata(movieSubjectName)
  val enrichedTripSchema = srClient.getSchemaById(enrichedTripMetadata.getId).rawSchema().asInstanceOf[Schema]

  // 3. convert CSV records to Avro messages (GenericRecord)
  val enrichedTripRecords: List[GenericData.Record] = enrichedTrips
    .map(_.split(",", -1))
    .map { fields =>
      new GenericRecordBuilder(enrichedTripSchema)
        .set("system_id", fields(0))
        .set("timezone", fields(1))
        .set("station_id", fields(2).toInt)
        .set("name", fields(3))
        .set("short_name", fields(4))
        .set("lat", fields(5).toDouble)
        .set("lon", fields(6).toDouble)
        .set("capacity", fields(7).toInt)
        .build()
    }


  // 4. Produce to kafka
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
  val producer = new KafkaProducer[String, GenericRecord](producerProperties)

  enrichedTripRecords
    .map(record => new ProducerRecord[String, GenericRecord]("test_enriched_trip",
      record.get("station_id").toString, record)
    ).foreach(producer.send)

  producer.flush()
  producer.close()
}
