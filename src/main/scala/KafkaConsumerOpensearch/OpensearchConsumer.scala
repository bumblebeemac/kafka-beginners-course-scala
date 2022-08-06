package KafkaConsumerOpensearch

import com.google.gson.JsonParser
import com.typesafe.scalalogging.Logger
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.{BasicCredentialsProvider, DefaultConnectionKeepAliveStrategy}
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.common.xcontent.XContentType

import java.net.URI
import java.time.Duration
import java.util.{Collections, Properties}

object OpensearchConsumer {

  // create an Opensearch client
  def createOpenSearchClient(): RestHighLevelClient = {
    val connString: String = "http://localhost:9200"
    val connURI: URI = URI.create(connString)
    val userInfo: String = connURI.getUserInfo

    if (userInfo == null) {
      // REST client without security
      new RestHighLevelClient(RestClient.builder(
        new HttpHost(connURI.getHost, connURI.getPort, "http")))
    } else {
      // REST client with security
      val auth: Array[String] = userInfo.split(":")

      val cp: CredentialsProvider = new BasicCredentialsProvider
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth(0), auth(1)))

      new RestHighLevelClient(RestClient.builder(
        new HttpHost(connURI.getHost, connURI.getPort, "http"))
        .setHttpClientConfigCallback(
          (httpAsyncClientBuilder: HttpAsyncClientBuilder) =>
            httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
              .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy)))
    }
  }

  def createKafkaConsumer(): KafkaConsumer[String, String] = {
    val bootstrapServer = "localhost:9092"
    val groupId = "consumer-opensearch-demo"

    // create Consumer properties
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // "none/earliest/latest"
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    new KafkaConsumer[String, String](properties)
  }

  def extradId(json: String): String = {
    // gson library
    JsonParser.parseString(json)
      .getAsJsonObject
      .get("meta")
      .getAsJsonObject
      .get("id")
      .getAsString
  }

  // main code logic

  // close things

  def demoOpensearchConsumer(): Unit = {

    val logger = Logger("WikimediaConsumer")

    val opensearchClient: RestHighLevelClient = createOpenSearchClient()

    // create kafka client
    val kafkaConsumer = createKafkaConsumer()

    try {
      val indexExists: Boolean = opensearchClient.indices().exists(new GetIndexRequest("wikimedia"),
        RequestOptions.DEFAULT)

      if (!indexExists) {
        // create index on Opensearch if it doesn't exist already
        val createIndexRequest: CreateIndexRequest = new CreateIndexRequest("wikimedia")
        opensearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT)
        logger.info("Wikimedia index has been created")
      } else {
        logger.info("Wikimedia index already exists")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }

    // subscribe the consumer to topic
    kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"))

    while(true) {

      val consumerRecords: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMillis(3000))

      val recordCount: Int = consumerRecords.count()
      logger.info("Received " + recordCount + " record(s).")

      val bulkRequest = new BulkRequest()

      consumerRecords.forEach { record =>
        try {
          // strategy 1
          // define an ID using Kafka Record coordinates
          val id = record.topic() + "_" + record.partition() + "_" + record.partition()

          // strategy 2
          // extract id from JSON value
          val id2 = extradId(record.value())

          val indexRequest = new IndexRequest("wikimedia")
            .source(record.value(), XContentType.JSON)
            .id(id2)
//          val response = opensearchClient.index(indexRequest, RequestOptions.DEFAULT)
          bulkRequest.add(indexRequest)

//          logger.info(response.getId)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      if (bulkRequest.numberOfActions() > 0) {
        val bulkResponse = opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
        logger.info("Inserted " + bulkResponse.getItems.length + " record(s).")

        try {
          Thread.sleep(1000)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      kafkaConsumer.commitSync()
      logger.info("Offsets have been committed.")

    }

    opensearchClient.close()
    kafkaConsumer.close()

  }

  def main(args: Array[String]): Unit = {
    demoOpensearchConsumer()
  }

}
