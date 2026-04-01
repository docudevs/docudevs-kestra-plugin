package io.kestra.plugin.docudevs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class BatchExtractTest {
    @Inject
    private RunContextFactory runContextFactory;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void runUploadsMultipleFilesProcessesBatchAndStoresExcel(@TempDir Path tempDir) throws Exception {
        byte[] excelBytes = new byte[] {0x50, 0x4b, 0x03, 0x04, 0x10, 0x20, 0x30};

        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-123\",\"maxConcurrency\":2}"));
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-123\",\"index\":0,\"totalDocuments\":2}"));
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-123\",\"index\":1,\"totalDocuments\":2}"));
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-123\",\"maxConcurrency\":2}"));
        server.enqueue(jsonResponse("{\"status\":\"COMPLETED\",\"guid\":\"batch-123\",\"tokenCount\":33}"));
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            .setBody(new Buffer().write(excelBytes)));
        server.enqueue(jsonResponse("{\"status\":\"COMPLETED\",\"guid\":\"batch-123\",\"tokenCount\":33}"));

        var file1 = tempDir.resolve("a.pdf");
        var file2 = tempDir.resolve("b.pdf");
        Files.writeString(file1, "a");
        Files.writeString(file2, "b");

        RunContext runContext = runContextFactory.of(Map.of(
            "apiKey", "batch-key"
        ));

        var task = BatchExtract.builder()
            .id("batch")
            .apiKey(new Property<>("{{ apiKey }}"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .files(Property.ofValue(List.of(file1.toString(), file2.toString())))
            .maxConcurrency(Property.ofValue(2))
            .resultFormat(Property.ofValue("EXCEL"))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(2))
            .command(new Property<>(Map.of(
                "ocr", "PREMIUM",
                "llm", "HIGH",
                "extractionMode", "STEPS",
                "prompt", "Extract invoices",
                "schema", "{\"type\":\"array\"}",
                "trace", true
            )))
            .build();

        var output = task.run(runContext);

        assertThat(output.getJobGuid(), is("batch-123"));
        assertThat(output.getStatus(), is("COMPLETED"));
        assertThat(output.getUploadedCount(), is(2));
        assertThat(output.getTokenCount(), is(33));
        assertThat(output.getResultFormat(), is("EXCEL"));
        assertThat(output.getResult(), is((Object) null));
        assertThat(output.getResultFile(), notNullValue());
        assertThat(output.getResultContentType(), is("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));

        try (var stored = runContext.storage().getFile(URI.create(output.getResultFile()))) {
            assertArrayEquals(excelBytes, stored.readAllBytes());
        }

        RecordedRequest createRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(createRequest.getPath(), is("/document/batch"));
        assertThat(createRequest.getHeader("Authorization"), is("batch-key"));
        JsonNode createBody = objectMapper.readTree(createRequest.getBody().readUtf8());
        assertThat(createBody.path("maxConcurrency").asInt(), is(2));

        RecordedRequest upload1 = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(upload1.getPath(), is("/document/batch/batch-123/upload"));
        assertThat(upload1.getHeader("Authorization"), is("batch-key"));

        RecordedRequest upload2 = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(upload2.getPath(), is("/document/batch/batch-123/upload"));

        RecordedRequest processRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(processRequest.getPath(), is("/document/batch/batch-123/process"));
        JsonNode processBody = objectMapper.readTree(processRequest.getBody().readUtf8());
        assertThat(processBody.path("ocr").asText(), is("PREMIUM"));
        assertThat(processBody.path("llm").asText(), is("HIGH"));
        assertThat(processBody.path("extractionMode").asText(), is("STEPS"));
        assertThat(processBody.path("prompt").asText(), is("Extract invoices"));
        assertThat(processBody.path("schema").asText(), is("{\"type\":\"array\"}"));
        assertThat(processBody.path("trace").asBoolean(), is(true));

        RecordedRequest status1 = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(status1.getPath(), is("/job/status/batch-123"));

        RecordedRequest resultRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(resultRequest.getPath(), is("/job/result/batch-123/excel"));

        RecordedRequest status2 = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(status2.getPath(), is("/job/status/batch-123"));
    }

    @Test
    void runFailsWhenBatchJobFails(@TempDir Path tempDir) throws Exception {
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-999\",\"maxConcurrency\":1}"));
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-999\",\"index\":0,\"totalDocuments\":1}"));
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-999\",\"maxConcurrency\":1}"));
        server.enqueue(jsonResponse("{\"status\":\"FAILED\",\"guid\":\"batch-999\",\"error\":\"invalid schema\"}"));

        var file = tempDir.resolve("a.pdf");
        Files.writeString(file, "a");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = BatchExtract.builder()
            .id("batch")
            .apiKey(Property.ofValue("batch-key"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .files(Property.ofValue(List.of(file.toString())))
            .resultFormat(Property.ofValue("JSON"))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(1))
            .command(new Property<>(Map.of("prompt", "Extract", "schema", "{\"type\":\"array\"}")))
            .build();

        var thrown = assertThrows(IllegalStateException.class, () -> task.run(runContext));

        assertThat(thrown.getMessage(), containsString("batch-999"));
        assertThat(thrown.getMessage(), containsString("FAILED"));
        assertThat(thrown.getMessage(), containsString("invalid schema"));
    }

    @Test
    void runFetchesConfigurationByNameForBatchProcessing(@TempDir Path tempDir) throws Exception {
        // Create batch
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-cfg\",\"maxConcurrency\":1}"));
        // Upload file
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-cfg\",\"index\":0,\"totalDocuments\":1}"));
        // Get configuration by name
        server.enqueue(jsonResponse("{\"ocr\":\"PREMIUM\",\"llm\":\"HIGH\",\"extractionMode\":\"STEPS\",\"prompt\":\"Config prompt\",\"schema\":\"{\\\"type\\\":\\\"array\\\"}\"}"));
        // Process batch
        server.enqueue(jsonResponse("{\"jobGuid\":\"batch-cfg\",\"maxConcurrency\":1}"));
        // Status poll: COMPLETED
        server.enqueue(jsonResponse("{\"status\":\"COMPLETED\",\"guid\":\"batch-cfg\",\"tokenCount\":15}"));
        // Result JSON
        server.enqueue(jsonResponse("[{\"name\":\"Acme\"}]"));
        // Post-wait status
        server.enqueue(jsonResponse("{\"status\":\"COMPLETED\",\"guid\":\"batch-cfg\",\"tokenCount\":15}"));

        var file = tempDir.resolve("doc.pdf");
        Files.writeString(file, "fake");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = BatchExtract.builder()
            .id("batch")
            .apiKey(Property.ofValue("test-key"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .files(Property.ofValue(List.of(file.toString())))
            .resultFormat(Property.ofValue("JSON"))
            .configurationName(Property.ofValue("my-batch-config"))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(2))
            .build();

        var output = task.run(runContext);

        assertThat(output.getJobGuid(), is("batch-cfg"));
        assertThat(output.getStatus(), is("COMPLETED"));
        assertThat(output.getTokenCount(), is(15));

        // Verify: create batch -> upload -> get config -> process batch -> status -> result -> status
        RecordedRequest createReq = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(createReq.getPath(), is("/document/batch"));

        RecordedRequest uploadReq = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(uploadReq.getPath(), is("/document/batch/batch-cfg/upload"));

        RecordedRequest configReq = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(configReq.getPath(), is("/configuration/my-batch-config"));
        assertThat(configReq.getMethod(), is("GET"));

        RecordedRequest processReq = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(processReq.getPath(), is("/document/batch/batch-cfg/process"));
        JsonNode processBody = objectMapper.readTree(processReq.getBody().readUtf8());
        assertThat(processBody.path("ocr").asText(), is("PREMIUM"));
        assertThat(processBody.path("prompt").asText(), is("Config prompt"));
    }

    @Test
    void rejectsConfigurationNameAndCommandTogether(@TempDir Path tempDir) throws Exception {
        var file = tempDir.resolve("doc.pdf");
        Files.writeString(file, "fake");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = BatchExtract.builder()
            .id("batch")
            .apiKey(Property.ofValue("test-key"))
            .baseUrl(Property.ofValue("http://localhost"))
            .files(Property.ofValue(List.of(file.toString())))
            .configurationName(Property.ofValue("my-config"))
            .command(new Property<>(Map.of("prompt", "Extract data")))
            .build();

        var thrown = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(thrown.getMessage(), containsString("mutually exclusive"));
    }

    private MockResponse jsonResponse(String body) {
        return new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(body);
    }
}
