package io.kestra.storage.minio;

import com.google.common.collect.Streams;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import io.minio.*;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
    public InputStream get(String tenantId, URI uri) throws IOException {
        try {
            return client().getObject(GetObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.getPath())
                .build()
            );
        } catch (Throwable e) {
            throw new FileNotFoundException(uri.toString() + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public boolean exists(String tenantId, URI uri) {
        // There is no way to check if an object exist so we gather the stat of the object which will throw an exception
        // if the object didn't exist.
        try {
            client().statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.getPath())
                .build()
            );
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Long size(String tenantId, URI uri) throws IOException {
        try {
            return client().statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.getPath())
                .build()
            )
                .size();
        } catch (Throwable e) {
            throw new FileNotFoundException(uri.toString() + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public Long lastModifiedTime(String tenantId, URI uri) throws IOException {
        try {
            return client().statObject(StatObjectArgs.builder()
                    .bucket(this.config.getBucket())
                    .object(tenantId + uri.getPath())
                    .build()
                )
                .lastModified().toInstant().toEpochMilli();
        } catch (Throwable e) {
            throw new FileNotFoundException(uri.toString() + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        try {
            client().putObject(PutObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.toString())
                .stream(data, -1, config.getPartSize())
                .build()
            );

            data.close();
        } catch (Throwable e) {
            throw new IOException(e);
        }

        return URI.create("kestra://" + uri.getPath());
    }

    @Override
    public boolean delete(String tenantId, URI uri) throws IOException {
        try {
            client().statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.getPath())
                .build()
            );

            client().removeObject(RemoveObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(tenantId + uri.getPath())
                .build()
            );

            return true;
        } catch (ErrorResponseException e) {
            if (e.errorResponse().code().equals("NoSuchKey")) {
                return false;
            }
            throw new IOException(e);
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        List<Pair<String, DeleteObject>> deleted = Streams
            .stream(client()
                .listObjects(ListObjectsArgs.builder()
                    .bucket(this.config.getBucket())
                    .prefix(tenantId + storagePrefix.getPath())
                    .recursive(true)
                    .build()
                )
            )
            .map(throwFunction(itemResult -> {
                try {
                    return Pair.of(itemResult.get().objectName(), new DeleteObject(itemResult.get().objectName()));
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }))
            .collect(Collectors.toList());

        Iterable<Result<DeleteError>> results = client().removeObjects(RemoveObjectsArgs.builder()
            .bucket(this.config.getBucket())
            .objects(deleted.stream().map(Pair::getRight).collect(Collectors.toList()))
            .build()
        );

        if (results.iterator().hasNext()) {
            throw new IOException("Unable to delete all files, failed on [" +
                Streams
                    .stream(results)
                    .map(throwFunction(r -> {
                        try {
                            return r.get().objectName();
                        } catch (Throwable e) {
                            throw new IOException(e);
                        }
                    }))
                    .collect(Collectors.joining(", ")) +
                "]");
        }

        return deleted
            .stream()
            .map(deleteObject -> URI.create("kestra:///" + deleteObject.getLeft().replace(tenantId + "/", "")))
            .collect(Collectors.toList());
    }
}
