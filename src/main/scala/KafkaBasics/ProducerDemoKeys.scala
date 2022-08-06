package KafkaBasics

import com.typesafe.scalalogging.{AnyLogging, Logger}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object ProducerDemoKeys extends AnyLogging {

  override protected val logger: Logger = Logger("ProducerDemoWithCallback") // does not work

  def demoKafkaProducer(): Unit = {

    // create Producer properties
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    // create the Producer
    val producer = new KafkaProducer[String, String](properties)

    // send data - asynchronous operation
    (1 to 10).foreach { i =>

      val topic = "demo_scala"
      val value = "hello world " + i
      val key = "id_" + i

      val producerRecord: ProducerRecord[String, String] =
        new ProducerRecord[String, String](topic, key, value)

      producer.send(producerRecord, (metadata: RecordMetadata, exception: Exception) => {
        // executes everytime a record is successfully sent or an exceptions thrown
        if (exception == null) logger.info("Received new metadata \n" +
          "Topic: " + metadata.topic() + "\n" +
          "Key: " + producerRecord.key() + "\n" +
          "Partition: " + metadata.partition() + "\n" +
          "Offset: " + metadata.offset() + "\n" +
          "Timestamp: " + metadata.timestamp() + "\n")
        else logger.error("Error while producing", exception)
      })
    }

    // flush and close the Producer
    producer.flush()
    producer.close() // just slowing close also flushes the Producer
  }

  def main(args: Array[String]): Unit = {
    demoKafkaProducer()
  }

}
