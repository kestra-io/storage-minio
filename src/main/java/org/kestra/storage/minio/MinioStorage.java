package org.kestra.storage.minio;

import io.micronaut.core.annotation.Introspected;
import io.minio.ErrorCode;
import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.errors.ErrorResponseException;
import org.kestra.core.storages.StorageInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.InvalidKeyException;
import java.util.HashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@MinioStorageEnabled
@Introspected
public class MinioStorage implements StorageInterface {
    @Inject
    MinioClientFactory factory;

    @Inject
    MinioConfig config;

    private MinioClient client() {
        return factory.of(config);
    }

    @Override
    public InputStream get(URI uri) throws FileNotFoundException  {
        try {
            return client().getObject(this.config.getBucket(), uri.getPath().substring(1));
        } catch (Throwable e) {
            throw new FileNotFoundException(uri.toString() + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public URI put(URI uri, InputStream data) throws IOException {
        try {
            client().putObject(
                this.config.getBucket(),
                uri.toString().substring(1),
                data,
                null,
                new HashMap<>(),
                null,
                null
            );

            data.close();
        } catch (Throwable e) {
            throw new IOException(e);
        }

        return URI.create("kestra://" + uri.getPath());
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        try {
            ObjectStat objectStat = client().statObject(this.config.getBucket(), uri.getPath().substring(1));
            client().removeObject(this.config.getBucket(), uri.getPath().substring(1));
            return true;
        } catch (ErrorResponseException e) {
            if (e.errorResponse().errorCode() == ErrorCode.NO_SUCH_KEY) {
                return false;
            }
            throw new IOException(e);
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }
}
