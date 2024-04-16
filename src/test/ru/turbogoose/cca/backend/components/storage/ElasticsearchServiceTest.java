package ru.turbogoose.cca.backend.components.storage;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import ru.turbogoose.cca.backend.common.util.Util;
import ru.turbogoose.cca.backend.components.datasets.util.FileConversionUtil;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

class ElasticsearchServiceTest {
    private static final Integer port = 9200;
    private static final String host = "localhost";
    private static final String fingerprint = "f63977e6b72f94e6b0ddc0280b11a3f86a403a79efde6a5a872ecc1395469ee2";
    private static final String username = "elastic";
    private static final String password = "Lnoqy3PF+xA03PWjguAx";

    public static ElasticsearchClient initElasticsearchClient() {
        SSLContext sslContext = TransportUtils
                .sslContextFromCaFingerprint(fingerprint);

        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password)
        );

        RestClient restClient = RestClient
                .builder(new HttpHost(host, port, "https"))
                .setHttpClientConfigCallback(hc -> hc
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credsProv)
                )
                .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    public static Searcher service = new ElasticsearchService(initElasticsearchClient(), new ObjectMapper());

    @Test
    public void runBenchmarks() {
        Map<String, Integer> runsByDatasets = Map.of(
//                "/Users/ilakonovalov/PycharmProjects/archive/dataset_1mb.csv", 3
//                ,"/Users/ilakonovalov/PycharmProjects/archive/dataset_10mb.csv", 3
                "/Users/ilakonovalov/PycharmProjects/archive/dataset_100mb.csv", 1
//                ,"/Users/ilakonovalov/PycharmProjects/archive/dataset_500mb.csv", 1
        );

        runsByDatasets.forEach(this::runBenchMark);
    }

    public void runBenchMark(String datasetFilename, int runs) {
        long totalTime = 0;
        for (int run = 0; run < runs; run++) {
            try (InputStream in = new FileInputStream(datasetFilename)) {
                String indexName = "test_index_" + Util.removeExtension(Path.of(datasetFilename).getFileName().toString());
                System.out.println("Start creating index " + indexName);
                long start = System.currentTimeMillis();
                service.createIndex(indexName, FileConversionUtil.readCsvDatasetToJson(in));
                long result = System.currentTimeMillis() - start;
                totalTime += result;
                System.out.printf("%d) Elapsed indexing time for dataset '%s': %d%n", run, datasetFilename, result);
                service.deleteIndex(indexName);
            } catch (IOException exc) {
                throw new RuntimeException(exc);
            }
        }
        System.out.println("Average indexing time: " + totalTime / runs);
        System.out.println("---------------------------------------------");

    }

}