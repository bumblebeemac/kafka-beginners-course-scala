package KafkaProducerWikimedia

import com.launchdarkly.eventsource.EventSource
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import java.net.URI
import java.util.Properties
import java.util.concurrent.TimeUnit


object WikimediaChangesProducer {

  val logger: Logger = Logger("ProducerDemo")

  val bootstrapServer = "localhost:9092"
  val topic = "wikimedia.recentchange"

  def demoWikimediaProducer(): Unit = {
    // create Producer properties
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    // set high throughput producer configs
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

    // create the Producer
    val kafkaProducer = new KafkaProducer[String, String](properties)

    val eventHandler = new WikimediaChangeHandler(kafkaProducer, topic)
    val url = "https://stream.wikimedia.org/v2/stream/recentchange"
    val builder: EventSource.Builder = new EventSource.Builder(eventHandler, URI.create(url))
    val eventSource: EventSource = builder.build()

    // start the Producer in another thread
    eventSource.start()

    // we produce for 10 minutes and block the program until then
    TimeUnit.MINUTES.sleep(10)
  }


  def main(args: Array[String]): Unit = {
    demoWikimediaProducer()
  }

}
