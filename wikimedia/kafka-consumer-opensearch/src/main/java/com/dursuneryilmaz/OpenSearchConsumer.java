package com.dursuneryilmaz;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        final String INDEX_NAME = "wikimedia.recentchange";
        // openSearch client
        RestHighLevelClient openSearchClient = getOpenSearchClient();

        // create index on OpenSearch if not exist
        if (!initOpenSearchIndex(log, INDEX_NAME, openSearchClient)) {
            return;
        }


        // kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singleton(INDEX_NAME));

        while (true) {
            ConsumerRecords<String, String> recordList = kafkaConsumer.poll(Duration.ofMillis(3000));
            int recordCount = recordList.count();
            log.info("Read record count: " + recordCount);

            for (ConsumerRecord<String, String> record : recordList) {
                // make consumer idempotent and transaction unique set an id to index request, get id from incoming data preferred
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset(); // or get id from kafka coordinates
                String id = getIdFromData(record.value());
                IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                        .source(record.value(), XContentType.JSON)
                        .id(id);

                // updates the existing request with same id
                IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                log.info(indexResponse.getId() + " : inserted");
            }
            // commit offsets manually, achieve at least once strategy
            kafkaConsumer.commitSync();
            log.info("Offset committed!");
        }
    }

    private static String getIdFromData(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static RestHighLevelClient getOpenSearchClient() {
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create("http://localhost:9200");
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static boolean initOpenSearchIndex(Logger log, String indexName, RestHighLevelClient openSearchClient) {
        try {
            boolean isIndexExist = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (!isIndexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("OpenSearch index created : " + indexName);
            } else {
                log.info("OpenSearch index already exist : " + indexName);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "opensearch-wikimedia";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }
}
