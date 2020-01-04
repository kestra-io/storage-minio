package org.kestra.storage.minio;

import com.google.common.io.CharStreams;
import io.micronaut.test.annotation.MicronautTest;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.storages.StorageObject;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class MinioStorageTest {
    @Inject
    MinioClientFactory clientFactory;

    @Inject
    MinioConfig config;

    @BeforeEach
    void init() throws Exception {
        MinioClient client = clientFactory.of(this.config);

        if (!client.bucketExists(config.getBucket())) {
            client.makeBucket(config.getBucket());
        }
    }

    @Inject
    StorageInterface storageInterface;

    private StorageObject putFile(URL resource, String path) throws Exception {
        return storageInterface.put(
            new URI(path),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );
    }

    @Test
    void get() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        this.putFile(resource, "file/storage/get.yml");

        InputStream get = storageInterface.get(new URI("file/storage/get.yml"));

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
        );
    }

    @Test
    void missing() {
        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(new URI("file/storage/missing.yml"));
        });
    }

    @Test
    void put() throws Exception {
        URL resource = MinioStorageTest.class.getClassLoader().getResource("application.yml");
        StorageObject put = this.putFile(resource, "file/storage/put.yml");
        InputStream get = storageInterface.get(new URI("file/storage/put.yml"));

//        assertThat(put.getUri(), is(new StorageObject(new URI("file/storage/get.yml"))));
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
        );
    }
}