package org.kestra.storage.minio;

import com.google.common.io.CharStreams;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kestra.core.storages.StorageInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.Objects;
import javax.inject.Inject;

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

        InputStream get = storageInterface.get(new URI("/file/storage/get.yml"));
        assertThat(CharStreams.toString(new InputStreamReader(get)), is(content));

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

        boolean delete = storageInterface.delete(put);
        assertThat(delete, is(true));

        delete = storageInterface.delete(put);
        assertThat(delete, is(false));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(new URI("/file/storage/put.yml"));
        });
    }
}
