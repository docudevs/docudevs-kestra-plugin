package io.kestra.plugin.docudevs;

import ai.docudevs.client.DocuDevsApiException;
import ai.docudevs.client.DocuDevsClient;
import ai.docudevs.client.DocuDevsTimeoutException;
import ai.docudevs.client.WaitOptions;
import ai.docudevs.client.generated.api.BatchApi;
import ai.docudevs.client.generated.api.JobApi;
import ai.docudevs.client.generated.internal.ApiClient;
import ai.docudevs.client.generated.internal.ApiException;
import ai.docudevs.client.generated.model.BatchCreateRequest;
import ai.docudevs.client.generated.model.UploadCommand;
import ai.docudevs.client.generated.model.ProcessingJob;
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

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Batch extract structured data with DocuDevs",
    description = """
        Creates a DocuDevs batch job, uploads multiple input files, starts processing,
        waits for completion, and returns JSON results or stores CSV/Excel outputs in Kestra storage.

        The `command` property is passed through to the DocuDevs `ProcessBatchRequest` model and
        supports the batch extraction parameters available in the API.
        """
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Batch extract to Excel",
            full = true,
            code = """
id: docudevs_batch_extract
namespace: company.team

tasks:
  - id: batch_extract
    type: io.kestra.plugin.docudevs.BatchExtract
    apiKey: "{{ secret('DOCUDEVS_API_KEY') }}"
    files:
      - /tmp/invoice-a.pdf
      - /tmp/invoice-b.pdf
    resultFormat: EXCEL
    maxConcurrency: 4
    command:
      extractionMode: STEPS
      ocr: PREMIUM
      llm: HIGH
      prompt: Extract invoice rows
      schema: '{"type":"array","items":{"type":"object"}}'
"""
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Batch extract from flow input list",
            full = true,
            code = """
id: docudevs_batch_extract_from_input
namespace: company.team

inputs:
  - id: files
    type: ARRAY
    itemType: STRING
    defaults:
      - /tmp/invoice-a.pdf
      - /tmp/invoice-b.pdf

tasks:
  - id: batch_extract
    type: io.kestra.plugin.docudevs.BatchExtract
    apiKey: "{{ secret('DOCUDEVS_API_KEY') }}"
    files: "{{ inputs.files }}"
    resultFormat: JSON
    command:
      extractionMode: STEPS
      ocr: PREMIUM
      prompt: Extract invoice data
      schema: '{"type":"array","items":{"type":"object"}}'
"""
        )
    }
)
public class BatchExtract extends AbstractDocuDevsTask implements RunnableTask<BatchExtract.Output> {
    @NotNull
    @Schema(
        title = "DocuDevs API key",
        description = "DocuDevs API key used for authentication."
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
        title = "Input files",
        description = "List of local file paths or Kestra internal storage URIs (`kestra://...`) to upload into the batch job."
    )
    private Property<List<String>> files;

    @Schema(
        title = "Batch processing command payload",
        description = """
            DocuDevs `ProcessBatchRequest` payload.
            Supports extraction parameters such as `ocr`, `llm`, `extractionMode`, `schema`,
            `prompt`, `mapReduce`, `tools`, `trace`, `pageRange`, and figure options.
            """
    )
    private Property<Map<String, Object>> command;

    @Schema(
        title = "Max concurrency",
        description = "Optional DocuDevs batch max concurrency."
    )
    private Property<Integer> maxConcurrency;

    @Builder.Default
    @Schema(
        title = "Result format",
        description = "Batch output format: `JSON`, `CSV`, or `EXCEL`."
    )
    private Property<String> resultFormat = Property.ofValue(ResultFormat.JSON.name());

    @Builder.Default
    @Schema(
        title = "Polling interval",
        description = "Interval between job status checks."
    )
    private Duration pollInterval = Duration.ofSeconds(2);

    @Builder.Default
    @Schema(
        title = "Timeout",
        description = "Maximum time to wait for the DocuDevs batch job to complete."
    )
    private Duration completionTimeout = Duration.ofMinutes(20);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rApiKey = runContext.render(this.apiKey).as(String.class).orElseThrow();
        var rBaseUrl = runContext.render(this.baseUrl).as(String.class).orElse(DEFAULT_BASE_URL);
        var rFiles = runContext.render(this.files).asList(String.class);
        var rResultFormat = this.resultFormat(runContext);
        var rTimeout = this.completionTimeout == null ? Duration.ofMinutes(20) : this.completionTimeout;
        var rPollInterval = this.pollInterval == null ? Duration.ofSeconds(2) : this.pollInterval;

        if (rFiles == null || rFiles.isEmpty()) {
            throw new IllegalArgumentException("files must contain at least one file");
        }
        if (rTimeout.isZero() || rTimeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be greater than zero");
        }
        if (rPollInterval.isZero() || rPollInterval.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be greater than zero");
        }

        var apiClient = this.apiClient(rBaseUrl, rApiKey, rTimeout);
        var batchApi = new BatchApi(apiClient);
        var jobApi = new JobApi(apiClient);
        var facadeClient = this.docuDevsClient(rBaseUrl, rApiKey);
        var waitOptions = WaitOptions.builder()
            .timeout(rTimeout)
            .pollInterval(rPollInterval)
            .build();

        var batchGuid = this.createBatch(batchApi, runContext);
        runContext.logger().info("Created DocuDevs batch job {}", batchGuid);

        try {
            var uploadedCount = this.uploadFiles(batchApi, runContext, batchGuid, rFiles);
            runContext.logger().info("Uploaded {} files to DocuDevs batch {}", uploadedCount, batchGuid);

            this.processBatch(batchApi, runContext, apiClient, batchGuid);
            runContext.logger().info("Started DocuDevs batch processing {}", batchGuid);

            var resultPayload = this.fetchBatchResult(runContext, apiClient, jobApi, facadeClient, batchGuid, rResultFormat, waitOptions);

            return Output.builder()
                .jobGuid(batchGuid)
                .status(resultPayload.status())
                .tokenCount(resultPayload.tokenCount())
                .uploadedCount(uploadedCount)
                .resultFormat(rResultFormat.name())
                .result(resultPayload.result())
                .resultFile(resultPayload.resultFile())
                .resultContentType(resultPayload.resultContentType())
                .build();
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                this.tryDeleteJob(facadeClient, batchGuid, runContext);
            }
            throw e;
        }
    }

    private ResultFormat resultFormat(RunContext runContext) throws Exception {
        var raw = runContext.render(this.resultFormat).as(String.class).orElse(ResultFormat.JSON.name());
        try {
            return ResultFormat.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported resultFormat: " + raw + " (expected JSON, CSV, or EXCEL)");
        }
    }

    private String createBatch(BatchApi batchApi, RunContext runContext) throws Exception {
        var request = new BatchCreateRequest();
        if (this.maxConcurrency != null) {
            var rMaxConcurrency = runContext.render(this.maxConcurrency).as(Integer.class).orElse(null);
            request.setMaxConcurrency(rMaxConcurrency);
        }

        try {
            var response = batchApi.createBatch(request);
            return Objects.requireNonNull(response.getJobGuid(), "DocuDevs batch create response missing jobGuid");
        } catch (ApiException e) {
            throw new IllegalStateException("DocuDevs batch create failed: " + e.getCode(), e);
        }
    }

    private int uploadFiles(BatchApi batchApi, RunContext runContext, String batchGuid, List<String> files) throws Exception {
        var uploadedCount = 0;
        for (var from : files) {
            var path = this.materializeInputFile(runContext, from);
            try {
                batchApi.uploadBatchDocument(batchGuid, path.toFile());
                uploadedCount++;
            } catch (ApiException e) {
                throw new IllegalStateException("DocuDevs batch upload failed for " + batchGuid + ": " + e.getCode(), e);
            }
        }
        return uploadedCount;
    }

    private void processBatch(BatchApi batchApi, RunContext runContext, ApiClient apiClient, String batchGuid) throws Exception {
        var request = this.processBatchRequest(runContext, apiClient);
        try {
            batchApi.processBatch(batchGuid, request);
        } catch (ApiException e) {
            throw new IllegalStateException("DocuDevs batch process request failed for " + batchGuid + ": " + e.getCode(), e);
        }
    }

    private UploadCommand processBatchRequest(RunContext runContext, ApiClient apiClient) throws Exception {
        UploadCommand request;
        if (this.command == null) {
            request = new UploadCommand();
        } else {
            var renderedCommand = runContext.render(this.command).asMap(String.class, Object.class);
            try {
                request = apiClient.getObjectMapper().convertValue(renderedCommand, UploadCommand.class);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid DocuDevs batch command payload", e);
            }
        }

        return request;
    }

    private ResultPayload fetchBatchResult(
        RunContext runContext,
        ApiClient apiClient,
        JobApi jobApi,
        DocuDevsClient facadeClient,
        String batchGuid,
        ResultFormat resultFormat,
        WaitOptions waitOptions
    ) throws Exception {
        try {
            Object result = null;
            String resultFile = null;
            String resultContentType = null;

            if (resultFormat == ResultFormat.JSON) {
                var jsonNode = this.retryOnTransientError(
                    () -> facadeClient.waitUntilReadyJson(batchGuid, waitOptions), batchGuid, runContext);
                result = apiClient.getObjectMapper().convertValue(jsonNode, Object.class);
            } else {
                var bytes = resultFormat == ResultFormat.EXCEL
                    ? this.retryOnTransientError(
                        () -> facadeClient.waitUntilReadyExcel(batchGuid, waitOptions), batchGuid, runContext)
                    : this.retryOnTransientError(
                        () -> facadeClient.waitUntilReadyCsv(batchGuid, waitOptions), batchGuid, runContext);
                var fileName = resultFormat == ResultFormat.EXCEL
                    ? "docudevs-batch-" + batchGuid + ".xlsx"
                    : "docudevs-batch-" + batchGuid + ".csv";
                resultContentType = resultFormat == ResultFormat.EXCEL
                    ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    : "text/csv";
                try (var input = new ByteArrayInputStream(bytes)) {
                    resultFile = runContext.storage().putFile(input, fileName).toString();
                }
            }

            var finalJob = this.getJobStatus(jobApi, batchGuid);
            return new ResultPayload(result, resultFile, resultContentType, finalJob.getStatus(), finalJob.getTokenCount());
        } catch (DocuDevsApiException e) {
            throw new IllegalStateException("DocuDevs batch job " + batchGuid + " failed: " + e.getResponseBody(), e);
        } catch (DocuDevsTimeoutException e) {
            throw new IllegalStateException("Timed out waiting for DocuDevs batch job: " + batchGuid, e);
        }
    }

    private ProcessingJob getJobStatus(JobApi jobApi, String guid) {
        try {
            return jobApi.getJobStatus(guid);
        } catch (ApiException e) {
            throw new IllegalStateException("Failed to fetch DocuDevs job status for " + guid + ": " + e.getCode(), e);
        }
    }

    private enum ResultFormat {
        JSON,
        CSV,
        EXCEL
    }

    private record ResultPayload(
        Object result,
        String resultFile,
        String resultContentType,
        String status,
        Integer tokenCount
    ) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "DocuDevs batch job GUID",
            description = "GUID of the DocuDevs batch processing job."
        )
        private final String jobGuid;

        @Schema(
            title = "Final job status",
            description = "Final DocuDevs batch job status."
        )
        private final String status;

        @Schema(
            title = "Token count",
            description = "Reported DocuDevs token count for the completed batch job."
        )
        private final Integer tokenCount;

        @Schema(
            title = "Uploaded file count",
            description = "Number of files uploaded to the DocuDevs batch job."
        )
        private final Integer uploadedCount;

        @Schema(
            title = "Result format",
            description = "Requested result format (`JSON`, `CSV`, `EXCEL`)."
        )
        private final String resultFormat;

        @Schema(
            title = "JSON result payload",
            description = "Batch JSON result payload when `resultFormat=JSON`."
        )
        private final Object result;

        @Schema(
            title = "Result file URI",
            description = "Kestra internal storage URI for the batch result file when `resultFormat=CSV` or `EXCEL`."
        )
        private final String resultFile;

        @Schema(
            title = "Result content type",
            description = "MIME content type of the batch result file."
        )
        private final String resultContentType;
    }
}
