package KafkaProducerWikimedia

import com.launchdarkly.eventsource.{EventHandler, MessageEvent}
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

class WikimediaChangeHandler(kafkaProducer: KafkaProducer[String, String], topic: String) extends EventHandler {

  val logger: Logger = Logger(classOf[WikimediaChangeHandler].getCanonicalName)

  override def onOpen(): Unit = ()

  override def onClosed(): Unit = kafkaProducer.close()

  override def onMessage(event: String, messageEvent: MessageEvent): Unit = {
    logger.info(messageEvent.getData)
    // asynchronous
    kafkaProducer.send(new ProducerRecord(topic, messageEvent.getData))
  }

  override def onComment(comment: String): Unit = ()

  override def onError(t: Throwable): Unit = logger.error(s"Error in stream reading: ${t.toString}")
}
