package KafkaBasics

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties

object ConsumerDemoWithShutdown {

  def demoKafkaConsumerWithShutdown(): Unit = {

    val logger = Logger("ConsumerDemoWithShutdown")

    logger.info("Hello Kafka World!")

    val groupId = "my-third-application"
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

    // get a reference to the current thread
    val mainThread: Thread = Thread.currentThread()

    // adding the shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...")
      kafkaConsumer.wakeup()

      // join the main thread to allow the execution of the code in the main thread
      try {
        mainThread.join()
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }))

    try {
      // subscribe consumer to our topics
      kafkaConsumer.subscribe(util.Arrays.asList(topic))

      // poll for new data
      while(true) {

        val records: ConsumerRecords[String, String] =
          kafkaConsumer.poll(Duration.ofMillis(1000))

        records.forEach { record =>
          logger.info("Key: " + record.key() + ", Value:" + record.value())
          logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
        }
      }
    } catch {
      case _: WakeupException => logger.info("Wake up exception") // we ignore as this is expected when closing consumer
      case _: Exception => logger.error("Unexpected exception")
    } finally {
      kafkaConsumer.close() // this will also commit offsets if need be
      logger.info("Consumer is now gracefully closed")
    }

  }

  def main(args: Array[String]): Unit = {
    demoKafkaConsumerWithShutdown()
  }

}
