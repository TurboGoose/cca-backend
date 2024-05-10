package ru.turbogoose.cca.backend.components.storage;

class ElasticsearchServiceTest {
//    private static final Integer port = 9200;
//    private static final String host = "localhost";
//    private static final String fingerprint = "f63977e6b72f94e6b0ddc0280b11a3f86a403a79efde6a5a872ecc1395469ee2";
//    private static final String username = "elastic";
//    private static final String password = "Lnoqy3PF+xA03PWjguAx";
//
//    public static ElasticsearchClient initElasticsearchClient() {
//        SSLContext sslContext = TransportUtils
//                .sslContextFromCaFingerprint(fingerprint);
//
//        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
//        credsProv.setCredentials(
//                AuthScope.ANY, new UsernamePasswordCredentials(username, password)
//        );
//
//        RestClient restClient = RestClient
//                .builder(new HttpHost(host, port, "https"))
//                .setHttpClientConfigCallback(hc -> hc
//                        .setSSLContext(sslContext)
//                        .setDefaultCredentialsProvider(credsProv)
//                )
//                .build();
//
//        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
//        return new ElasticsearchClient(transport);
//    }
//
//    public static Storage<JsonNode, JsonNode> service = new ElasticsearchService(initElasticsearchClient());
//
//    @Test
//    public void runBenchmarks() {
//        Map<String, Integer> runsByDatasets = new LinkedHashMap<>();
//        runsByDatasets.put("/Users/ilakonovalov/PycharmProjects/archive/dataset_1mb.csv", 3);
//        runsByDatasets.put("/Users/ilakonovalov/PycharmProjects/archive/dataset_10mb.csv", 3);
////        runsByDatasets.put("/Users/ilakonovalov/PycharmProjects/archive/dataset_100mb.csv", 1);
////        runsByDatasets.put("/Users/ilakonovalov/PycharmProjects/archive/dataset_500mb.csv", 1);
//
//        runsByDatasets.forEach(this::runBenchMark);
//    }
//
//    public void runBenchMark(String datasetFilename, int runs) {
//        long totalTime = 0;
//        for (int run = 0; run < runs; run++) {
//            Path path = Path.of(datasetFilename);
//            InternalStorageInfo info = service.create();
//            System.out.println("Start testing dataset " + path);
//
//            try (Stream<JsonNode> dataStream = CsvUtil.readCsvAsJson(path)) {
//                long start = System.currentTimeMillis();
//                service.fill(info, dataStream);
//                long result = System.currentTimeMillis() - start;
//                totalTime += result;
//                System.out.printf("%d) Elapsed indexing time for dataset '%s': %d%n", run, datasetFilename, result);
//                service.delete(info);
//            }
//        }
//        System.out.println("Average indexing time: " + totalTime / runs);
//        System.out.println("---------------------------------------------");
//
//    }
}