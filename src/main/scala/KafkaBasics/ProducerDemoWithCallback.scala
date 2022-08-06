package KafkaBasics

import com.typesafe.scalalogging.{AnyLogging, Logger}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object ProducerDemoWithCallback extends AnyLogging {

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
    val callback = new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        // executes everytime a record is successfully sent or an exception s thrown
        if (exception == null) logger.info("Received new metadata \n" +
          "Topic: " + metadata.topic() + "\n" +
          "Partition: " + metadata.partition() + "\n" +
          "Offset: " + metadata.offset() + "\n" +
          "Timestamp: " + metadata.timestamp() + "\n")
        else logger.error("Error while producing", exception)}
    }

    (1 to 10).foreach { i =>
      val producerRecord: ProducerRecord[String, String] =
        new ProducerRecord[String, String]("demo_scala", "hello world" + i)

      producer.send(producerRecord, callback)
    }


    // flush and close the Producer
    producer.flush()
    producer.close() // just slowing close also flushes the Producer
  }

  def main(args: Array[String]): Unit = {
    demoKafkaProducer()
  }

}
