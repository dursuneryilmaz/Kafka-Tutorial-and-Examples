package com.dursuneryilmaz;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        final String INDEX_NAME = "wikimedia.recentchange";
        // openSearch client
        RestHighLevelClient openSearchClient = getOpenSearchClient();

        // create index on OpenSearch if not exist
        initOpenSearchIndex(log, INDEX_NAME, openSearchClient);

        // kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
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

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static void initOpenSearchIndex(Logger log, String indexName, RestHighLevelClient openSearchClient) throws IOException {
        try (openSearchClient) {
            boolean isIndexExist = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (!isIndexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("OpenSearch index created : " + indexName);
            } else {
                log.info("OpenSearch index already exist : " + indexName);
            }
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-wikimedia";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }
}
