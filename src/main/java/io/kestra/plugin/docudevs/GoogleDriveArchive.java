package io.kestra.plugin.docudevs;

import com.google.api.client.googleapis.apache.v2.GoogleApacheHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@Getter
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Schema(
    title = "Archive processed Google Drive files",
    description = """
        Move original files from the source folder to the archive folder after publishing their results.
        Run this task after the destination upload and pass the successful input references from Batch Extract.
        """
)
@Plugin(examples = @Example(
    title = "Move original files using a manifest of successful downloads",
    full = true,
    code = """
        id: archive_processed_drive_file
        namespace: company.team

        inputs:
          - id: sourceFolderId
            type: STRING
          - id: archiveFolderId
            type: STRING
          - id: filesById
            type: JSON
          - id: successfulFiles
            type: ARRAY
            itemType: STRING

        tasks:
          - id: archive
            type: io.kestra.plugin.docudevs.GoogleDriveArchive
            serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
            sourceFolderId: "{{ inputs.sourceFolderId }}"
            archiveFolderId: "{{ inputs.archiveFolderId }}"
            filesById: "{{ inputs.filesById | toJson }}"
            successfulFiles: "{{ inputs.successfulFiles | toJson }}"
        """
))
public class GoogleDriveArchive extends Task implements RunnableTask<VoidOutput> {
    @NotNull
    @Schema(title = "Google service-account JSON")
    private Property<String> serviceAccount;

    @NotNull
    @Schema(title = "Source folder ID")
    private Property<String> sourceFolderId;

    @NotNull
    @Schema(title = "Archive folder ID")
    private Property<String> archiveFolderId;

    @NotNull
    @Schema(title = "Downloaded files", description = "Map from Drive file ID to downloaded storage URI.")
    private Property<Map<String, String>> filesById;

    @NotNull
    @Schema(title = "Successful input URIs", description = "Original file references reported by Batch Extract.")
    private Property<List<String>> successfulFiles;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rSourceFolder = runContext.render(sourceFolderId).as(String.class).orElseThrow();
        var rArchiveFolder = runContext.render(archiveFolderId).as(String.class).orElseThrow();
        if (rSourceFolder.isBlank() || rArchiveFolder.isBlank() || rSourceFolder.equals(rArchiveFolder)) {
            throw new IllegalArgumentException("The Drive source and archive folders must be distinct");
        }

        var rSuccessfulFiles = runContext.render(successfulFiles).asList(String.class);
        if (rSuccessfulFiles.isEmpty()) {
            return null;
        }
        var rFilesById = runContext.render(filesById).asMap(String.class, String.class);
        var fileIds = successfulDriveIds(rFilesById, rSuccessfulFiles);
        var rServiceAccount = runContext.render(serviceAccount).as(String.class).orElseThrow();
        var transport = GoogleApacheHttpTransport.newTrustedTransport();
        try {
            var credentials = ServiceAccountCredentials.fromStream(
                new ByteArrayInputStream(rServiceAccount.getBytes(StandardCharsets.UTF_8))
            ).createScoped(List.of(DriveScopes.DRIVE));
            var auth = new HttpCredentialsAdapter(credentials);
            var drive = new Drive.Builder(transport, GsonFactory.getDefaultInstance(), request -> {
                auth.initialize(request);
                request.setConnectTimeout(10_000);
                request.setReadTimeout(30_000);
            }).setApplicationName("DocuDevs").build();

            moveFiles(drive, rSourceFolder, rArchiveFolder, fileIds);
            runContext.logger().info("Archived {} processed Google Drive files", fileIds.size());
        } finally {
            transport.shutdown();
        }
        return null;
    }

    static List<String> successfulDriveIds(Map<String, String> filesById, List<String> successfulFiles) {
        var idsByUri = new HashMap<String, String>();
        filesById.forEach((id, uri) -> {
            if (idsByUri.putIfAbsent(uri, id) != null) {
                throw new IllegalArgumentException("Multiple Drive file IDs refer to the same download");
            }
        });

        return successfulFiles.stream().map(uri -> {
            var id = idsByUri.get(uri);
            if (id == null) {
                throw new IllegalArgumentException("A successful batch input has no matching Drive file ID");
            }
            return id;
        }).distinct().toList();
    }

    static void moveFiles(Drive drive, String sourceFolder, String archiveFolder, List<String> fileIds)
        throws IOException {
        var files = drive.files();
        for (var fileId : fileIds) {
            var parents = files.get(fileId).setSupportsAllDrives(true).setFields("parents").execute().getParents();
            if (parents != null && parents.contains(archiveFolder)) {
                continue;
            }
            if (parents == null || !parents.contains(sourceFolder)) {
                throw new IllegalStateException("Drive file " + fileId + " is no longer in the source folder");
            }
            files.update(fileId, new File())
                .setSupportsAllDrives(true)
                .setAddParents(archiveFolder)
                .setRemoveParents(sourceFolder)
                .setFields("id")
                .execute();
        }
    }
}
