package KafkaBasics

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object ProducerDemo {

  def demoKafkaProducer(): Unit = {

    val logger = Logger("ProducerDemo")

    logger.info("Hello Kafka World!")

    // create Producer properties
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    // create the Producer
    val producer = new KafkaProducer[String, String](properties)

    // create a Producer Record
    val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String]("demo_scala", "hello world")

    // send data - asynchronous operation
    producer.send(producerRecord)

    // flush and close the Producer
    producer.flush()
    producer.close() // just slowing close also flushes the Producer
  }


  def main(args: Array[String]): Unit = {
    demoKafkaProducer()
  }

}
