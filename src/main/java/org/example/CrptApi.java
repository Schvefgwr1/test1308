package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrptApi {

    private final RateLimiter rateLimiter;
    private final DocumentProcessor documentProcessor;
    private final RequestSender requestSender;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.rateLimiter = new RateLimiter(requestLimit, timeUnit.toMillis(1));
        this.documentProcessor = new DocumentProcessorImpl();
        this.requestSender = new ApiRequestSender("ismp.crpt.ru");
    }

    public void createDocument(Object document, String signature) throws InterruptedException {
        try {
            rateLimiter.acquire();
            Document documentObj = documentProcessor.documentProcess(document, signature);
            String documentID = requestSender.sendCreateDocument(documentObj);
            System.out.println(documentID);
        } catch (IOException | IllegalStateException e) {
            System.err.println(e.getMessage());
        } finally {
            rateLimiter.release();
        }
    }

    public void shutdown() {
        rateLimiter.shutdown();
    }

    /**
     *  Интерфейс обработки документов
     */
    private interface DocumentProcessor {
        Document documentProcess(Object document, String signature);
    }

    private enum DocumentFormat {
        MANUAL, XML, CSV
    }
    private enum ProductGroup {
        clothes, shoes, tobacco, perfumery, tires, electronics, pharma, milk, bicycle, wheelchairs
    }

    private enum ProductType {
        AGGREGATION_DOCUMENT, AGGREGATION_DOCUMENT_CSV, AGGREGATION_DOCUMENT_XML, DISAGGREGATION_DOCUMENT,
        DISAGGREGATION_DOCUMENT_CSV, DISAGGREGATION_DOCUMENT_XML, REAGGREGATION_DOCUMENT, REAGGREGATION_DOCUMENT_CSV,
        REAGGREGATION_DOCUMENT_XML, LP_INTRODUCE_GOODS, LP_SHIP_GOODS, LP_SHIP_GOODS_CSV, LP_SHIP_GOODS_XML,
        LP_INTRODUCE_GOODS_CSV, LP_INTRODUCE_GOODS_XML, LP_ACCEPT_GOODS, LP_ACCEPT_GOODS_XML, LK_REMARK, LK_REMARK_CSV,
        LK_REMARK_XML, LK_RECEIPT, LK_RECEIPT_XML, LK_RECEIPT_CSV, LP_GOODS_IMPORT, LP_GOODS_IMPORT_CSV,
        LP_GOODS_IMPORT_XML, LP_CANCEL_SHIPMENT, LP_CANCEL_SHIPMENT_CSV, LP_CANCEL_SHIPMENT_XML, LK_KM_CANCELLATION,
        LK_KM_CANCELLATION_CSV, LK_KM_CANCELLATION_XML, LK_APPLIED_KM_CANCELLATION, LK_APPLIED_KM_CANCELLATION_CSV,
        LK_APPLIED_KM_CANCELLATION_XML, LK_CONTRACT_COMMISSIONING, LK_CONTRACT_COMMISSIONING_CSV,
        LK_CONTRACT_COMMISSIONING_XML, LK_INDI_COMMISSIONING, LK_INDI_COMMISSIONING_CSV, LK_INDI_COMMISSIONING_XML,
        LP_SHIP_RECEIPT, LP_SHIP_RECEIPT_CSV, LP_SHIP_RECEIPT_XML, OST_DESCRIPTION, OST_DESCRIPTION_CSV,
        OST_DESCRIPTION_XML, CROSSBORDER, CROSSBORDER_CSV, CROSSBORDER_XML, LP_INTRODUCE_OST, LP_INTRODUCE_OST_CSV,
        LP_INTRODUCE_OST_XML, LP_RETURN, LP_RETURN_CSV, LP_RETURN_XML, LP_SHIP_GOODS_CROSSBORDER,
        LP_SHIP_GOODS_CROSSBORDER_CSV, LP_SHIP_GOODS_CROSSBORDER_XML, LP_CANCEL_SHIPMENT_CROSSBORDER
    }

    @Builder
    @Getter
    private static class Document {
        private final String signature;
        private final Object content;
        private final DocumentFormat documentFormat;
        private final ProductGroup productGroup;
        private final ProductType productType;
    }

    private class DocumentProcessorImpl implements DocumentProcessor {
        @Override
        public Document documentProcess(Object document, String signature) {
            /* Необходимые доп. действия с документом, создаем просто для примера */
            return Document.builder()
                    .signature(signature)
                    .content(document)
                    .documentFormat(DocumentFormat.MANUAL)
                    .productGroup(ProductGroup.milk)
                    .productType(ProductType.AGGREGATION_DOCUMENT)
                    .build();
        }
    }

    /**
     * Интерфейс отправки запросов
     */
    private interface RequestSender {
        String sendCreateDocument(Document document) throws IOException;
    }

    private class ApiRequestSender implements RequestSender {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final String host;

        public ApiRequestSender(String host) {
            this.host = host;
        }

        @Override
        public String sendCreateDocument(Document document) throws IOException {
            URL url = buildUrl(
                    "/api/v3/lk/documents/create",
                    document.getProductGroup() != null ? Map.of("pg", document.getProductGroup().name()) : Map.of()
            );

            HttpsURLConnection conn = openConnection(url, "POST");

            String productDocumentJson = objectMapper.writeValueAsString(document.getContent());
            Map<String, Object> requestBody = Map.of(
                    "document_format", document.getDocumentFormat().name(),
                    "product_document", productDocumentJson,
                    "signature", document.getSignature(),
                    "type", document.getProductType().name()
            );
            sendRequest(conn, requestBody);

            String responseJson = readResponse(conn);

            if (conn.getResponseCode() == 200) {
                var node = objectMapper.readTree(responseJson);
                String value = node.path("value").asText(null);
                if (value == null) {
                    throw new IOException("Unexpected response format: " + responseJson);
                }
                return value;
            } else {
                throw handleError(responseJson);
            }
        }

        private URL buildUrl(String methodPath, Map<String, String> queryParams) throws IOException {
            StringBuilder urlString = new StringBuilder("https://" + host + methodPath);
            if (!queryParams.isEmpty()) {
                urlString.append("?");
                queryParams.forEach((key, value) -> urlString.append(key).append("=").append(value));
            }
            return new URL(urlString.toString());
        }

        private HttpsURLConnection openConnection(URL url, String method) throws IOException {
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
            conn.setRequestMethod(method);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            return conn;
        }

        private void sendRequest(HttpsURLConnection conn, Map<String, Object> requestBody) throws IOException {
            String jsonRequest = objectMapper.writeValueAsString(requestBody);
            try (var os = conn.getOutputStream()) {
                byte[] input = jsonRequest.getBytes(StandardCharsets.UTF_8);
                os.write(input);
            }
        }

        private String readResponse(HttpsURLConnection conn) throws IOException {
            try (var is = (conn.getResponseCode() == 200 ? conn.getInputStream() : conn.getErrorStream())) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        private IOException handleError(String responseJson) {
            JsonNode node;
            try {
                node = objectMapper.readTree(responseJson);
            } catch (JsonProcessingException e) {
                return new IOException("Unsupported exception: " + e.getMessage());
            }
            String errorMsg = node.path("error").asText("Unknown error");
            String description = node.path("description").asText("");
            return new IOException("Failed: " + errorMsg + ". " + description);
        }
    }

    /**
     *  Лимитер запросов
     */
    private class RateLimiter {
        private final Semaphore semaphore;
        private final ScheduledExecutorService scheduler;
        private final AtomicInteger requestCount;
        private final int requestLimit;
        private final long intervalMillis;

        public RateLimiter(int limit, long intervalMillis) {
            this.requestLimit = limit;
            this.intervalMillis = intervalMillis;

            this.semaphore = new Semaphore(requestLimit, true);
            this.scheduler = Executors.newScheduledThreadPool(1);
            this.requestCount = new AtomicInteger(0);

            scheduler.scheduleAtFixedRate(() -> {
                semaphore.release(requestLimit - semaphore.availablePermits());
                requestCount.set(0);
            }, this.intervalMillis, this.intervalMillis, TimeUnit.MILLISECONDS);
        }

        public void acquire() throws InterruptedException, IllegalStateException {
            semaphore.acquire();
            if (requestCount.incrementAndGet() > requestLimit) {
                semaphore.release();
                throw new IllegalStateException("Request limit exceeded");
            }
        }

        public void release() {
            semaphore.release();
        }

        public void shutdown() {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 5); // 5 запросов в секунду

        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 20; i++) {
            executor.submit(() -> {
                try {
                    Object document = new HashMap<>().put("Test", "Test");
                    String signature = "dummy_signature";
                    api.createDocument(document, signature);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        api.shutdown();
    }
}
