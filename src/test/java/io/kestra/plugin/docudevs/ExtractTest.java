package io.kestra.plugin.docudevs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ExtractTest {
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
    void runUploadsProcessesAndReturnsResultWithFullCommandPayload(@TempDir Path tempDir) throws Exception {
        // Upload response
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-123\",\"status\":\"PENDING\"}"));
        // Process response
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-123\",\"status\":\"PENDING\"}"));
        // Status poll: PROCESSING
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"PROCESSING\"}"));
        // Status poll: COMPLETED
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"COMPLETED\",\"guid\":\"job-123\",\"tokenCount\":77}"));
        // Result JSON
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"invoiceNumber\":\"INV-001\",\"total\":42.5}"));
        // Post-wait status (for metadata)
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"COMPLETED\",\"guid\":\"job-123\",\"tokenCount\":77}"));

        var inputFile = tempDir.resolve("invoice.pdf");
        Files.writeString(inputFile, "fake-pdf");

        RunContext runContext = runContextFactory.of(Map.of("apiKey", "secret-docudevs-key"));

        var task = Extract.builder()
            .id("extract")
            .apiKey(new Property<>("{{ apiKey }}"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .from(Property.ofValue(inputFile.toString()))
            .dependsOn(Property.ofValue("upstream-guid"))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(2))
            .command(new Property<>(Map.ofEntries(
                Map.entry("ocr", "PREMIUM"),
                Map.entry("llm", "HIGH"),
                Map.entry("extractionMode", "STEPS"),
                Map.entry("schema", "{\"type\":\"object\"}"),
                Map.entry("prompt", "Extract invoice data"),
                Map.entry("barcodes", true),
                Map.entry("mimeType", "application/pdf"),
                Map.entry("describeFigures", true),
                Map.entry("extractFigures", true),
                Map.entry("trace", true),
                Map.entry("pageRange", List.of(1, 3)),
                Map.entry("tools", List.of(Map.of(
                    "type", "KNOWLEDGE_BASE_SEARCH",
                    "config", Map.of("knowledgeBaseId", "kb-1", "topK", 5)
                ))),
                Map.entry("mapReduce", Map.of(
                    "enabled", true,
                    "parallelProcessing", true,
                    "splitType", "MARKDOWN_HEADER",
                    "splitHeaderLevel", 2,
                    "pagesPerChunk", 3,
                    "overlapPages", 1,
                    "dedupKey", "row.id",
                    "stopWhenEmpty", true,
                    "emptyChunkGrace", 2,
                    "header", Map.of(
                        "enabled", true,
                        "pageLimit", 2,
                        "includeInRows", false,
                        "pageIndices", List.of(0, -1),
                        "schema", "{\"type\":\"object\"}",
                        "schemaFileName", "header-schema.json",
                        "prompt", "Extract header",
                        "promptFileName", "header-prompt.txt",
                        "rowPromptAugmentation", "Use header values for row extraction"
                    )
                ))
            )))
            .build();

        var output = task.run(runContext);

        assertThat(output.getGuid(), is("job-123"));
        assertThat(output.getStatus(), is("COMPLETED"));
        assertThat(output.getTokenCount(), is(77));
        assertThat(output.getResult(), instanceOf(Map.class));
        assertThat(((Map<?, ?>) output.getResult()).get("invoiceNumber"), is("INV-001"));

        RecordedRequest uploadRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(uploadRequest, notNullValue());
        assertThat(uploadRequest.getPath(), is("/document/upload"));
        assertThat(uploadRequest.getHeader("Authorization"), is("secret-docudevs-key"));

        RecordedRequest processRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(processRequest, notNullValue());
        assertThat(processRequest.getPath(), is("/document/process/job-123?dependsOn=upstream-guid"));
        assertThat(processRequest.getHeader("Authorization"), is("secret-docudevs-key"));

        JsonNode payload = objectMapper.readTree(processRequest.getBody().readUtf8());
        assertThat(payload.path("ocr").asText(), is("PREMIUM"));
        assertThat(payload.path("llm").asText(), is("HIGH"));
        assertThat(payload.path("extractionMode").asText(), is("STEPS"));
        assertThat(payload.path("schema").asText(), is("{\"type\":\"object\"}"));
        assertThat(payload.path("prompt").asText(), is("Extract invoice data"));
        assertThat(payload.path("barcodes").asBoolean(), is(true));
        assertThat(payload.path("mimeType").asText(), is("application/pdf"));
        assertThat(payload.path("describeFigures").asBoolean(), is(true));
        assertThat(payload.path("extractFigures").asBoolean(), is(true));
        assertThat(payload.path("trace").asBoolean(), is(true));
        assertThat(payload.path("pageRange").get(0).asInt(), is(1));
        assertThat(payload.path("tools").get(0).path("type").asText(), is("KNOWLEDGE_BASE_SEARCH"));
        assertThat(payload.path("mapReduce").path("enabled").asBoolean(), is(true));
        assertThat(payload.path("mapReduce").path("parallelProcessing").asBoolean(), is(true));
        assertThat(payload.path("mapReduce").path("splitType").asText(), is("MARKDOWN_HEADER"));
        assertThat(payload.path("mapReduce").path("splitHeaderLevel").asInt(), is(2));
        assertThat(payload.path("mapReduce").path("pagesPerChunk").asInt(), is(3));
        assertThat(payload.path("mapReduce").path("overlapPages").asInt(), is(1));
        assertThat(payload.path("mapReduce").path("dedupKey").asText(), is("row.id"));
        assertThat(payload.path("mapReduce").path("stopWhenEmpty").asBoolean(), is(true));
        assertThat(payload.path("mapReduce").path("emptyChunkGrace").asInt(), is(2));
        assertThat(payload.path("mapReduce").path("header").path("enabled").asBoolean(), is(true));
        assertThat(payload.path("mapReduce").path("header").path("pageLimit").asInt(), is(2));
        assertThat(payload.path("mapReduce").path("header").path("includeInRows").asBoolean(), is(false));
        assertThat(payload.path("mapReduce").path("header").path("pageIndices").get(1).asInt(), is(-1));
        assertThat(payload.path("mapReduce").path("header").path("schemaFileName").asText(), is("header-schema.json"));
        assertThat(payload.path("mapReduce").path("header").path("promptFileName").asText(), is("header-prompt.txt"));
        assertThat(payload.path("mapReduce").path("header").path("rowPromptAugmentation").asText(), is("Use header values for row extraction"));
    }

    @Test
    void runFailsWhenJobFails(@TempDir Path tempDir) throws Exception {
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-999\",\"status\":\"PENDING\"}"));
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-999\",\"status\":\"PENDING\"}"));
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"FAILED\",\"guid\":\"job-999\",\"error\":\"invalid schema\"}"));

        var inputFile = tempDir.resolve("input.txt");
        Files.writeString(inputFile, "hello");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = Extract.builder()
            .id("extract")
            .apiKey(Property.ofValue("test-key"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .from(Property.ofValue(inputFile.toString()))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(1))
            .command(new Property<>(Map.of("prompt", "Extract", "schema", "{\"type\":\"object\"}")))
            .build();

        var thrown = assertThrows(IllegalStateException.class, () -> task.run(runContext));

        assertThat(thrown.getMessage(), containsString("job-999"));
        assertThat(thrown.getMessage(), containsString("FAILED"));
        assertThat(thrown.getMessage(), containsString("invalid schema"));
    }

    @Test
    void runUsesConfigurationNameEndpoint(@TempDir Path tempDir) throws Exception {
        // Upload response
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-cfg\",\"status\":\"PENDING\"}"));
        // Process-with-configuration response
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"guid\":\"job-cfg\",\"status\":\"PENDING\"}"));
        // Status poll: COMPLETED
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"COMPLETED\",\"guid\":\"job-cfg\",\"tokenCount\":10}"));
        // Result JSON
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"name\":\"Acme Corp\"}"));
        // Post-wait status
        server.enqueue(new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"status\":\"COMPLETED\",\"guid\":\"job-cfg\",\"tokenCount\":10}"));

        var inputFile = tempDir.resolve("doc.pdf");
        Files.writeString(inputFile, "fake");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = Extract.builder()
            .id("extract")
            .apiKey(Property.ofValue("test-key"))
            .baseUrl(Property.ofValue(server.url("/").toString()))
            .from(Property.ofValue(inputFile.toString()))
            .configurationName(Property.ofValue("invoice-extraction"))
            .pollInterval(Duration.ofMillis(5))
            .completionTimeout(Duration.ofSeconds(2))
            .build();

        var output = task.run(runContext);

        assertThat(output.getGuid(), is("job-cfg"));
        assertThat(output.getStatus(), is("COMPLETED"));
        assertThat(output.getTokenCount(), is(10));
        assertThat(((Map<?, ?>) output.getResult()).get("name"), is("Acme Corp"));

        // Verify upload
        RecordedRequest uploadRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(uploadRequest, notNullValue());
        assertThat(uploadRequest.getPath(), is("/document/upload"));

        // Verify process call used configuration endpoint
        RecordedRequest processRequest = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(processRequest, notNullValue());
        assertThat(processRequest.getPath(), containsString("/document/process/job-cfg/with-configuration/invoice-extraction"));
    }

    @Test
    void rejectsConfigurationNameAndCommandTogether(@TempDir Path tempDir) throws Exception {
        var inputFile = tempDir.resolve("doc.pdf");
        Files.writeString(inputFile, "fake");

        RunContext runContext = runContextFactory.of(Map.of());

        var task = Extract.builder()
            .id("extract")
            .apiKey(Property.ofValue("test-key"))
            .baseUrl(Property.ofValue("http://localhost"))
            .from(Property.ofValue(inputFile.toString()))
            .configurationName(Property.ofValue("my-config"))
            .command(new Property<>(Map.of("prompt", "Extract data")))
            .build();

        var thrown = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(thrown.getMessage(), containsString("mutually exclusive"));
    }
}
