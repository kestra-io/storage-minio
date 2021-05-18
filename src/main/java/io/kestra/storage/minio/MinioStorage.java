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
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

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
    public InputStream get(URI uri) throws FileNotFoundException {
        try {
            return client().getObject(GetObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(uri.getPath().substring(1))
                .build()
            );
        } catch (Throwable e) {
            throw new FileNotFoundException(uri.toString() + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public Long size(URI uri) throws IOException {
        try {
            return client().statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(uri.getPath().substring(1))
                .build()
            )
                .size();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    @Override
    public URI put(URI uri, InputStream data) throws IOException {
        try {
            client().putObject(PutObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(uri.toString().substring(1))
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
    public boolean delete(URI uri) throws IOException {
        try {
            client().statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(uri.getPath().substring(1))
                .build()
            );

            client().removeObject(RemoveObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(uri.getPath().substring(1))
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
    public List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
        List<Pair<String, DeleteObject>> deleted = Streams
            .stream(client()
                .listObjects(ListObjectsArgs.builder()
                    .bucket(this.config.getBucket())
                    .prefix(storagePrefix.getPath().substring(1))
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
            .map(deleteObject -> URI.create("kestra:///" + deleteObject.getLeft()))
            .collect(Collectors.toList());
    }
}
