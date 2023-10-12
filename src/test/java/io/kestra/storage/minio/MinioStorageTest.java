package io.kestra.storage.minio;

import com.google.common.io.CharStreams;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kestra.core.storages.StorageInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
class MinioStorageTest {
    @Inject
    MinioClientFactory clientFactory;

    @Inject
    MinioConfig config;

    @BeforeEach
    void init() throws Exception {
        MinioClient client = clientFactory.of(this.config);

        if (!client.bucketExists(BucketExistsArgs.builder().bucket(config.getBucket()).build())) {
            client.makeBucket(MakeBucketArgs.builder().bucket(config.getBucket()).build());
        }
    }

    @Inject
    StorageInterface storageInterface;

    private URI putFile(String tenantId, URL resource, String path) throws Exception {
        return storageInterface.put(
            tenantId,
            new URI(path),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );
    }

    @Test
    void get() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));
        String tenantId = IdUtils.create();

        get(tenantId, resource, content);
    }

    void get_NoTenant() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));
        String tenantId = null;

        get(tenantId, resource, content);
    }

    private void get(String tenantId, URL resource, String content) throws Exception {
        this.putFile(tenantId, resource, "/file/storage/get.yml");

        URI item = new URI("/file/storage/get.yml");
        InputStream get = storageInterface.get(tenantId, item);
        assertThat(CharStreams.toString(new InputStreamReader(get)), is(content));
        assertTrue(storageInterface.exists(tenantId, item));
        assertThat(storageInterface.size(tenantId, item), is((long) content.length()));
        assertThat(storageInterface.lastModifiedTime(tenantId, item), notNullValue());

        InputStream getScheme = storageInterface.get(tenantId, new URI("kestra:///file/storage/get.yml"));
        assertThat(CharStreams.toString(new InputStreamReader(getScheme)), is(content));
    }

    @Test
    void missing() {
        String tenantId = IdUtils.create();

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/file/storage/missing.yml"));
        });
    }

    @Test
    void put() throws Exception {
        String tenantId = IdUtils.create();

        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        URI put = this.putFile(tenantId, resource, "/file/storage/put.yml");
        InputStream get = storageInterface.get(tenantId, new URI("/file/storage/put.yml"));

        assertThat(put.toString(), is(new URI("kestra:///file/storage/put.yml").toString()));
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
        );

        assertThat(storageInterface.size(tenantId, new URI("/file/storage/put.yml")), is(234L));

        assertThrows(FileNotFoundException.class, () -> {
            assertThat(storageInterface.size(tenantId, new URI("/file/storage/muissing.yml")), is(76L));
        });

        boolean delete = storageInterface.delete(tenantId, put);
        assertThat(delete, is(true));

        delete = storageInterface.delete(tenantId, put);
        assertThat(delete, is(false));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/file/storage/put.yml"));
        });
    }

    @Test
    void deleteByPrefix() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        String tenantId = IdUtils.create();

        deleteByPrefix(tenantId, resource);
    }

    @Test
    void deleteByPrefix_NoTenant() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        String tenantId = IdUtils.create();

        deleteByPrefix(tenantId, resource);
    }

    private void deleteByPrefix(String tenantId, URL resource) throws Exception {
        List<String> path = Arrays.asList(
            "/file/storage/root.yml",
            "/file/storage/level1/1.yml",
            "/file/storage/level1/level2/1.yml"
        );

        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        List<URI> deleted = storageInterface.deleteByPrefix(tenantId, new URI("/file/storage/"));

        assertThat(deleted, containsInAnyOrder(path.stream().map(s -> URI.create("kestra://" + s)).toArray()));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/file/storage/"));
        });

        path
            .forEach(s -> {
                assertThrows(FileNotFoundException.class, () -> {
                    storageInterface.get(tenantId, new URI(s));
                });
            });
    }

    @Test
    void deleteByPrefixNoResult() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        List<URI> deleted = storageInterface.deleteByPrefix(tenantId, new URI("/" + prefix + "/storage/"));
        assertThat(deleted.size(), is(0));
    }
}
