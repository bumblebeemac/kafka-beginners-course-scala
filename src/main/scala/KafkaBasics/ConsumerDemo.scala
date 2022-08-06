package KafkaBasics

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties

object ConsumerDemo {

  def demoKafkaConsumer(): Unit = {

    val logger = Logger("ConsumerDemo")

    logger.info("Hello Kafka World!")

    val groupId = "my-second-application"
    val topic = "demo_scala"

    // create Consumer properties
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // "none/earliest/latest"

    // create Consumer
    val kafkaConsumer = new KafkaConsumer[String, String](properties)

    // subscribe consumer to our topics
    kafkaConsumer.subscribe(util.Arrays.asList(topic))

    // poll for new data
    while(true) {

      logger.info("Polling")

      val records: ConsumerRecords[String, String] =
        kafkaConsumer.poll(Duration.ofMillis(1000))

      records.forEach {record =>
        logger.info("Key: " + record.key() + ", Value:" + record.value())
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
      }
    }


  }


  def main(args: Array[String]): Unit = {
    demoKafkaConsumer()
  }

}
