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

    private URI putFile(URL resource, String path) throws Exception {
        return storageInterface.put(
            new URI(path),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );
    }

    @Test
    void get() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        this.putFile(resource, "/file/storage/get.yml");

        URI item = new URI("/file/storage/get.yml");
        InputStream get = storageInterface.get(item);
        assertThat(CharStreams.toString(new InputStreamReader(get)), is(content));
        assertTrue(storageInterface.exists(item));
        assertThat(storageInterface.size(item), is((long) content.length()));
        assertThat(storageInterface.lastModifiedTime(item), notNullValue());

        InputStream getScheme = storageInterface.get(new URI("kestra:///file/storage/get.yml"));
        assertThat(CharStreams.toString(new InputStreamReader(getScheme)), is(content));
    }

    @Test
    void missing() {
        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(new URI("/file/storage/missing.yml"));
        });
    }

    @Test
    void put() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        URI put = this.putFile(resource, "/file/storage/put.yml");
        InputStream get = storageInterface.get(new URI("/file/storage/put.yml"));

        assertThat(put.toString(), is(new URI("kestra:///file/storage/put.yml").toString()));
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
        );

        assertThat(storageInterface.size(new URI("/file/storage/put.yml")), is(234L));

        assertThrows(FileNotFoundException.class, () -> {
            assertThat(storageInterface.size(new URI("/file/storage/muissing.yml")), is(76L));
        });

        boolean delete = storageInterface.delete(put);
        assertThat(delete, is(true));

        delete = storageInterface.delete(put);
        assertThat(delete, is(false));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(new URI("/file/storage/put.yml"));
        });
    }

    @Test
    void deleteByPrefix() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/file/storage/root.yml",
            "/file/storage/level1/1.yml",
            "/file/storage/level1/level2/1.yml"
        );

        path.forEach(throwConsumer(s -> this.putFile(resource, s)));

        List<URI> deleted = storageInterface.deleteByPrefix(new URI("/file/storage/"));

        assertThat(deleted, containsInAnyOrder(path.stream().map(s -> URI.create("kestra://" + s)).toArray()));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(new URI("/file/storage/"));
        });

        path
            .forEach(s -> {
                assertThrows(FileNotFoundException.class, () -> {
                    storageInterface.get(new URI(s));
                });
            });
    }

    @Test
    void deleteByPrefixNoResult() throws Exception {
        String prefix = IdUtils.create();

        List<URI> deleted = storageInterface.deleteByPrefix(new URI("/" + prefix + "/storage/"));
        assertThat(deleted.size(), is(0));
    }
}
