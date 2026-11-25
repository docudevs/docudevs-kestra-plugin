package io.kestra.plugin.docudevs;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class GoogleDriveArchiveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void movesOnlySuccessfulFilesAndRecognizesAnAlreadyMovedFile() throws Exception {
        try (var server = new MockWebServer()) {
            server.start();
            server.enqueue(parents("inbox"));
            server.enqueue(parents("processed"));
            server.enqueue(parents("processed"));

            var drive = new Drive.Builder(new ApacheHttpTransport(), GsonFactory.getDefaultInstance(), null)
                .setRootUrl(server.url("/").toString())
                .setApplicationName("test")
                .build();
            var filesById = Map.of("succeeded", "kestra:///a", "failed", "kestra:///b", "already", "kestra:///c");
            var fileIds = GoogleDriveArchive.successfulDriveIds(filesById, List.of("kestra:///a", "kestra:///c"));

            GoogleDriveArchive.moveFiles(drive, "inbox", "processed", fileIds);

            var get = server.takeRequest();
            assertEquals("/drive/v3/files/succeeded", get.getRequestUrl().encodedPath());
            assertEquals("true", get.getRequestUrl().queryParameter("supportsAllDrives"));

            var move = server.takeRequest();
            assertEquals("PATCH", move.getMethod());
            assertEquals("/drive/v3/files/succeeded", move.getRequestUrl().encodedPath());
            assertEquals("processed", move.getRequestUrl().queryParameter("addParents"));
            assertEquals("inbox", move.getRequestUrl().queryParameter("removeParents"));
            assertEquals("true", move.getRequestUrl().queryParameter("supportsAllDrives"));

            assertEquals("/drive/v3/files/already", server.takeRequest().getRequestUrl().encodedPath());
            assertEquals(3, server.getRequestCount());
        }
    }

    @Test
    void refusesToMoveFilesFromAnUnexpectedFolder() throws Exception {
        try (var server = new MockWebServer()) {
            server.start();
            server.enqueue(parents("other"));
            var drive = new Drive.Builder(new ApacheHttpTransport(), GsonFactory.getDefaultInstance(), null)
                .setRootUrl(server.url("/").toString())
                .setApplicationName("test")
                .build();

            assertThrows(IllegalStateException.class,
                () -> GoogleDriveArchive.moveFiles(drive, "inbox", "processed", List.of("some-id")));
            assertEquals(1, server.getRequestCount());
        }
    }

    @Test
    void skipsAuthenticationWhenThereAreNoSuccessfulFiles() throws Exception {
        var task = GoogleDriveArchive.builder()
            .id("archive")
            .sourceFolderId(Property.ofValue("inbox"))
            .archiveFolderId(Property.ofValue("processed"))
            .filesById(new Property<>("{{ filesById | toJson }}"))
            .successfulFiles(new Property<>("{{ successfulFiles | toJson }}"))
            .build();

        task.run(runContextFactory.of(Map.of("filesById", Map.of("failed", "kestra:///b"), "successfulFiles", List.of())));
    }

    @Test
    void refusesUnmappedOrAmbiguousReferences() {
        assertThrows(IllegalArgumentException.class, () ->
            GoogleDriveArchive.successfulDriveIds(Map.of("a", "kestra:///a"), List.of("kestra:///unknown")));
        assertThrows(IllegalArgumentException.class, () ->
            GoogleDriveArchive.successfulDriveIds(Map.of("a", "kestra:///a", "b", "kestra:///a"), List.of("kestra:///a")));
    }

    private MockResponse parents(String id) {
        return new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody("{\"parents\":[\"" + id + "\"]}");
    }
}
