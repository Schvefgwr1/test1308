package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Класс для работы с API Честного знака.
 * Поддерживает ограничение количества запросов (rate limiting).
 */
public class CrptApi {

    private final RateLimiter rateLimiter;
    private final DocumentProcessor documentProcessor;
    private final RequestSender requestSender;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.rateLimiter = new RateLimiter(requestLimit, timeUnit);
        this.documentProcessor = new DocumentProcessorImpl();
        this.requestSender = new ApiRequestSender("ismp.crpt.ru");
    }

    /**
     * Создание документа для ввода в оборот товара, произведенного в РФ.
     *
     * @param document  объект документа (будет сериализован в JSON)
     * @param signature цифровая подпись
     * @throws InterruptedException если поток прерван при ожидании свободного лимита
     * @throws CrptApiException     если произошла ошибка API
     */
    public void createDocument(Object document, String signature) throws InterruptedException, CrptApiException {
        rateLimiter.acquire();

        Document documentObj = documentProcessor.documentProcess(document, signature);
        String documentID = requestSender.sendCreateDocument(documentObj);
        System.out.println("Создан документ с ID: " + documentID);
    }

    // Exceptions

    public static class CrptApiException extends Exception {
        public CrptApiException(String message) { super(message); }
        public CrptApiException(String message, Throwable cause) { super(message, cause); }
    }

    public static class ApiRequestException extends CrptApiException {
        public ApiRequestException(String message) { super(message); }
    }

    // Document

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
        LP_INTRODUCE_GOODS, AGGREGATION_DOCUMENT, // Для примера оставлено немного
        // ... остальные значения из документации
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

    private static class DocumentProcessorImpl implements DocumentProcessor {
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

    // RequestSender

    private interface RequestSender {
        String sendCreateDocument(Document document) throws CrptApiException;
    }

    private static class ApiRequestSender implements RequestSender {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final HttpClient httpClient = HttpClient.newHttpClient();
        private final String host;

        public ApiRequestSender(String host) {
            this.host = host;
        }

        @Override
        public String sendCreateDocument(Document document) throws CrptApiException {
            try {
                URI uri = buildUri(
                        "/api/v3/lk/documents/create",
                        document.getProductGroup() != null ? Map.of("pg", document.getProductGroup().name()) : Map.of()
                );

                Map<String, Object> body = Map.of(
                        "document_format", document.getDocumentFormat().name(),
                        "product_document", document.getContent(),
                        "signature", document.getSignature(),
                        "type", document.getProductType().name()
                );

                String jsonRequest = objectMapper.writeValueAsString(body);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonRequest))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    JsonNode node = objectMapper.readTree(response.body());
                    return node.path("value").asText("UNKNOWN_ID");
                } else {
                    throw new ApiRequestException("Ошибка API: " + response.body());
                }

            } catch (IOException | InterruptedException e) {
                throw new CrptApiException("Ошибка при отправке запроса", e);
            }
        }

        private URI buildUri(String path, Map<String, String> queryParams) {
            StringBuilder sb = new StringBuilder("https://").append(host).append(path);
            if (!queryParams.isEmpty()) {
                sb.append("?");
                queryParams.forEach((k, v) -> sb.append(URLEncoder.encode(k, StandardCharsets.UTF_8))
                        .append("=")
                        .append(URLEncoder.encode(v, StandardCharsets.UTF_8))
                        .append("&"));
                sb.setLength(sb.length() - 1);
            }
            return URI.create(sb.toString());
        }
    }

    // RateLimiter

    /**
     * Реализация ограничителя запросов (RateLimiter) на основе семафора.
     * Не более N запросов за указанный интервал времени.
     */
    private static class RateLimiter {
        private final Semaphore semaphore;
        private final int requestLimit;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        public RateLimiter(int requestLimit, TimeUnit timeUnit) {
            this.requestLimit = requestLimit;
            this.semaphore = new Semaphore(requestLimit, true);

            long intervalMillis = timeUnit.toMillis(1);

            // Каждые intervalMillis сбрасываем семафор
            scheduler.scheduleAtFixedRate(() -> {
                synchronized (semaphore) {
                    int toRelease = this.requestLimit - semaphore.availablePermits();
                    if (toRelease > 0) {
                        semaphore.release(toRelease);
                    }
                }
            }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        }

        public void acquire() throws InterruptedException {
            semaphore.acquire();
        }
    }

    // Main

    public static void main(String[] args) throws InterruptedException {
        // Разрешаем максимум 3 запроса в секунду
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 3);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 10; i++) {
            int idx = i;
            executor.submit(() -> {
                try {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("description", "Test document #" + idx);
                    api.createDocument(doc, "dummy_signature");
                } catch (Exception e) {
                    System.err.println("Ошибка при создании документа #" + idx + ": " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }
}
