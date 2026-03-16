package io.kestra.plugin.docudevs;

import ai.docudevs.client.DocuDevsApiException;
import ai.docudevs.client.DocuDevsClient;
import ai.docudevs.client.DocuDevsTimeoutException;
import ai.docudevs.client.WaitOptions;
import ai.docudevs.client.generated.api.DocumentApi;
import ai.docudevs.client.generated.api.JobApi;
import ai.docudevs.client.generated.internal.ApiClient;
import ai.docudevs.client.generated.internal.ApiException;
import ai.docudevs.client.generated.model.UploadCommand;
import ai.docudevs.client.generated.model.UploadResponse;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Extract structured data from a document with DocuDevs",
    description = """
        Uploads a document to DocuDevs, submits an extraction request, waits for completion,
        and returns the extracted result.

        The `command` property is passed through to DocuDevs' `/document/process/{guid}` request
        body and supports all fields from the API `UploadCommand` model.
        """
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Extract invoice data",
            full = true,
            code = """
id: docudevs_extract
namespace: company.team

tasks:
  - id: extract
    type: io.kestra.plugin.docudevs.Extract
    apiKey: "{{ secret('DOCUDEVS_API_KEY') }}"
    from: /tmp/invoice.pdf
    command:
      extractionMode: STEPS
      ocr: PREMIUM
      llm: HIGH
      prompt: Extract invoice fields
      schema: '{"type":"object","properties":{"invoiceNumber":{"type":"string"}}}'
"""
        )
    }
)
public class Extract extends AbstractDocuDevsTask implements RunnableTask<Extract.Output> {

    @NotNull
    @Schema(
        title = "DocuDevs API key",
        description = """
            DocuDevs API key used for authentication.
            Use a Kestra secret expression for this value.
            """
    )
    private Property<String> apiKey;

    @Builder.Default
    @Schema(
        title = "DocuDevs API base URL",
        description = "DocuDevs API base URL."
    )
    private Property<String> baseUrl = Property.ofValue(DEFAULT_BASE_URL);

    @NotNull
    @PluginProperty(internalStorageURI = true)
    @Schema(
        title = "Input file path or Kestra internal storage URI",
        description = """
            Path to the input document, or a Kestra internal storage URI (`kestra://...`).
            """
    )
    private Property<String> from;

    @Schema(
        title = "Depends-on job GUID",
        description = """
            Optional DocuDevs job GUID to pass as the `dependsOn` query parameter.
            """
    )
    private Property<String> dependsOn;

    @Schema(
        title = "Extraction command payload",
        description = """
            Full DocuDevs `UploadCommand` payload sent to `/document/process/{guid}`.
            This supports all extraction parameters available in the API, including nested
            `mapReduce`, `mapReduce.header`, `tools`, `trace`, and `pageRange`.
            """
    )
    private Property<Map<String, Object>> command;

    @Builder.Default
    @Schema(
        title = "Polling interval",
        description = "Interval between job status checks."
    )
    private Duration pollInterval = Duration.ofSeconds(2);

    @Builder.Default
    @Schema(
        title = "Timeout",
        description = "Maximum time to wait for the DocuDevs job to complete."
    )
    private Duration completionTimeout = Duration.ofMinutes(10);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rApiKey = runContext.render(this.apiKey).as(String.class).orElseThrow();
        var rBaseUrl = runContext.render(this.baseUrl).as(String.class).orElse(DEFAULT_BASE_URL);
        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        var rDependsOn = this.dependsOn == null ? null : runContext.render(this.dependsOn).as(String.class).orElse(null);
        var rTimeout = this.completionTimeout == null ? Duration.ofMinutes(10) : this.completionTimeout;
        var rPollInterval = this.pollInterval == null ? Duration.ofSeconds(2) : this.pollInterval;

        if (rTimeout.isZero() || rTimeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be greater than zero");
        }
        if (rPollInterval.isZero() || rPollInterval.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be greater than zero");
        }

        var apiClient = this.apiClient(rBaseUrl, rApiKey, rTimeout);
        var documentApi = new DocumentApi(apiClient);
        var jobApi = new JobApi(apiClient);
        var facadeClient = this.docuDevsClient(rBaseUrl, rApiKey);
        var uploadCommand = this.uploadCommand(runContext, apiClient);
        var uploadFile = this.materializeInputFile(runContext, rFrom);

        runContext.logger().info("Uploading document to DocuDevs: {}", uploadFile.getFileName());

        var uploadResponse = this.callUpload(documentApi, uploadFile);
        var guid = Objects.requireNonNull(uploadResponse.getGuid(), "DocuDevs upload response missing guid");

        runContext.logger().info("Submitting DocuDevs extraction job {}", guid);

        this.callProcess(documentApi, guid, rDependsOn, uploadCommand);

        var waitOptions = WaitOptions.builder()
            .timeout(rTimeout)
            .pollInterval(rPollInterval)
            .build();

        try {
            var jsonResult = this.retryOnTransientError(
                () -> facadeClient.waitUntilReadyJson(guid, waitOptions), guid, runContext);
            var result = apiClient.getObjectMapper().convertValue(jsonResult, Object.class);
            var finalJob = jobApi.getJobStatus(guid);

            return Output.builder()
                .guid(guid)
                .status(finalJob.getStatus())
                .tokenCount(finalJob.getTokenCount())
                .result(result)
                .build();
        } catch (DocuDevsApiException e) {
            throw new IllegalStateException("DocuDevs job " + guid + " failed: " + e.getResponseBody(), e);
        } catch (DocuDevsTimeoutException e) {
            throw new IllegalStateException("Timed out waiting for DocuDevs job: " + guid, e);
        } catch (ApiException e) {
            throw new IllegalStateException("Failed to fetch DocuDevs job metadata for " + guid + ": " + e.getCode(), e);
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                this.tryDeleteJob(facadeClient, guid, runContext);
            }
            throw e;
        }
    }

    private UploadCommand uploadCommand(RunContext runContext, ApiClient apiClient) throws Exception {
        if (this.command == null) {
            return new UploadCommand();
        }

        var renderedCommand = runContext.render(this.command).asMap(String.class, Object.class);

        try {
            return apiClient.getObjectMapper().convertValue(renderedCommand, UploadCommand.class);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid DocuDevs command payload", e);
        }
    }

    private UploadResponse callUpload(DocumentApi documentApi, Path uploadFile) {
        try {
            return documentApi.uploadDocument(uploadFile.toFile());
        } catch (ApiException e) {
            throw new IllegalStateException("DocuDevs upload failed: " + e.getCode(), e);
        }
    }

    private void callProcess(DocumentApi documentApi, String guid, String dependsOn, UploadCommand uploadCommand) {
        try {
            var response = documentApi.processDocument(guid, dependsOn, uploadCommand);
            if (response != null && response.getError() != null && !response.getError().isBlank()) {
                throw new IllegalStateException("DocuDevs process request returned error for " + guid + ": " + response.getError());
            }
        } catch (ApiException e) {
            throw new IllegalStateException("DocuDevs process request failed for " + guid + ": " + e.getCode(), e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "DocuDevs job GUID",
            description = "GUID of the DocuDevs processing job."
        )
        private final String guid;

        @Schema(
            title = "Final job status",
            description = "Final DocuDevs job status."
        )
        private final String status;

        @Schema(
            title = "Token count",
            description = "Reported DocuDevs token count for the completed job."
        )
        private final Integer tokenCount;

        @Schema(
            title = "Extraction result",
            description = "Result returned by the DocuDevs extraction job."
        )
        private final Object result;
    }
}
