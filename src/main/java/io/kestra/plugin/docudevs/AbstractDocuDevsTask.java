package io.kestra.plugin.docudevs;

import ai.docudevs.client.DocuDevsApiException;
import ai.docudevs.client.DocuDevsClient;
import ai.docudevs.client.generated.internal.ApiClient;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

@SuperBuilder
@Getter
@NoArgsConstructor
abstract class AbstractDocuDevsTask extends Task {
    protected static final String DEFAULT_BASE_URL = "https://api.docudevs.ai";

    protected ApiClient apiClient(String baseUrl, String apiKey, Duration readTimeout) {
        var apiClient = new ApiClient();
        apiClient.updateBaseUri(this.normalizeBaseUrl(baseUrl));
        apiClient.setConnectTimeout(Duration.ofSeconds(20));
        apiClient.setReadTimeout(readTimeout);
        apiClient.setRequestInterceptor(builder -> builder.header("Authorization", apiKey));
        return apiClient;
    }

    protected DocuDevsClient docuDevsClient(String baseUrl, String apiKey) {
        return DocuDevsClient.builder()
            .baseUrl(this.normalizeBaseUrl(baseUrl))
            .apiKey(apiKey)
            .build();
    }

    protected Path materializeInputFile(RunContext runContext, String fromValue) throws Exception {
        var uri = this.parseUri(fromValue);

        if (uri != null && uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("kestra")) {
            try (var inputStream = runContext.storage().getFile(uri)) {
                var fileName = this.fileName(uri, "document.bin");
                return runContext.workingDir().createFile(UUID.randomUUID() + "-" + fileName, inputStream);
            }
        }

        var path = uri != null && uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("file")
            ? Path.of(uri)
            : Path.of(fromValue);

        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Input file not found: " + fromValue);
        }

        return path;
    }

    protected void tryDeleteJob(DocuDevsClient client, String guid, RunContext runContext) {
        try {
            client.deleteJob(guid);
            runContext.logger().info("Cancelled DocuDevs job {} after interrupt", guid);
        } catch (Exception ignored) {
            runContext.logger().warn("Failed to cancel DocuDevs job {} after interrupt", guid);
        }
    }

    protected <T> T retryOnTransientError(Supplier<T> action, String guid, RunContext runContext) {
        try {
            return action.get();
        } catch (DocuDevsApiException e) {
            if (e.getStatusCode() >= 500) {
                runContext.logger().warn("Transient error polling DocuDevs job {}, retrying: {}", guid, e.getMessage());
                return action.get();
            }
            throw e;
        }
    }

    protected String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return DEFAULT_BASE_URL;
        }
        if (baseUrl.endsWith("/")) {
            return baseUrl.substring(0, baseUrl.length() - 1);
        }
        return baseUrl;
    }

    private URI parseUri(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        if (value.startsWith("/") || value.startsWith("./") || value.startsWith("../")) {
            return null;
        }

        try {
            return URI.create(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private String fileName(URI uri, String fallback) {
        var path = uri.getPath();
        if (path == null || path.isBlank()) {
            return fallback;
        }

        var fileName = Path.of(path).getFileName();
        return fileName == null ? fallback : fileName.toString();
    }
}
