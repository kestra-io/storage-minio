package io.kestra.storage.minio;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;
import io.kestra.storage.minio.domains.ProxyConfiguration;
import io.kestra.storage.minio.internal.BytesSize;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwFunction;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Jacksonized
@Getter
@Plugin
@Plugin.Id("minio")
public class MinioStorage implements StorageInterface, MinioConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MinioStorage.class);

    private String endpoint;
    private int port;
    private String accessKey;
    private String secretKey;
    private String region;
    private boolean secure;
    private String bucket;
    private boolean vhost;
    private ProxyConfiguration proxyConfiguration;
    @Builder.Default
    private BytesSize partSize = new BytesSize(1024 * 1024 * 5);

    @Getter(AccessLevel.PRIVATE)
    private MinioClient minioClient;

    /**
     * {@inheritDoc}
     **/
    @Override
    public void init() {
        this.minioClient = MinioClientFactory.of(this);
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        try {
            String path = getPath(tenantId, uri);

            return this.minioClient.getObject(GetObjectArgs.builder()
                .bucket(this.bucket)
                .object(path)
                .build()
            );
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        try {
            String path = getPath(tenantId, uri);
            Map<String, String> metadata = MetadataUtils.toRetrievedMetadata(this.minioClient.statObject(StatObjectArgs.builder()
                .bucket(this.bucket)
                .object(path)
                .build()).userMetadata());

            return new StorageObject(metadata, this.get(tenantId, namespace, uri));
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) throws IOException {
        String internalStoragePrefix = getPath(tenantId, prefix);
        String prefixForMinio = toPrefix(internalStoragePrefix, false);
        return keysForPrefix(prefixForMinio, true, includeDirectories)
            .map(name -> URI.create("kestra://" + prefix.getPath() + name.substring(internalStoragePrefix.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        try {
            String prefix = toPrefix(getPath(tenantId, uri), true);
            List<FileAttributes> list = keysForPrefix(prefix, false, true)
                .map(throwFunction(this::getFileAttributes))
                .toList();
            if (list.isEmpty()) {
                // this will throw FileNotFound if there is no directory
                this.getAttributes(tenantId, namespace, uri);
            }
            return list;
        } catch (FileNotFoundException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Stream<String> keysForPrefix(String prefix, boolean recursive, boolean includeDirectories) throws IOException {
        try {
            Iterable<Result<Item>> results = this.minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .prefix(prefix)
                .delimiter("/")
                .recursive(recursive)
                .build());
            return StreamSupport.stream(results.spliterator(), false)
                .map(throwFunction(o -> o.get().objectName()))
                .filter(name -> {
                    name = name.substring(prefix.length());
                    // Remove recursive result and requested dir
                    return !name.isEmpty()
                        && !Objects.equals(name, prefix)
                        && !name.equals("/")
                        && (recursive || Path.of(name).getParent() == null)
                        && (includeDirectories || !name.endsWith("/"));
                });
        } catch (MinioException e) {
            throw reThrowMinioStorageException(prefix, e);
        } catch (FileNotFoundException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    @Override
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        String path = getPath(tenantId, uri);
        return exists(path);
    }

    private boolean exists(String path) {
        // There is no way to check if an object exist so we gather the stat of the object which will throw an exception
        // if the object didn't exist.
        try {
            this.minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucket)
                .object(path)
                .build()
            );
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.endsWith("/") && !exists(tenantId, namespace, uri)) {
            // if key does not exist we try to get the "directory" (directory are just object ending with /)
            path = path + "/";
        }
        return getFileAttributes(path);
    }

    private FileAttributes getFileAttributes(String path) throws IOException {
        try {
            StatObjectResponse stat = this.minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucket)
                .object(path)
                .build()
            );
            return MinioFileAttributes.builder()
                .fileName(new File(path).getName())
                .isDirectory(path.endsWith("/"))
                .stat(stat)
                .build();
        } catch (MinioException e) {
            throw reThrowMinioStorageException(path, e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        String path = getPath(tenantId, uri);
        mkdirs(path);
        try (InputStream data = storageObject.inputStream()) {
            this.minioClient.putObject(PutObjectArgs.builder()
                .bucket(bucket)
                .object(path)
                .userMetadata(MetadataUtils.toStoredMetadata(storageObject.metadata()))
                .stream(data, -1, partSize.value())
                .build()
            );
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }

        return URI.create("kestra://" + uri.getRawPath());
    }

    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/") + 1);
        }

        // check if it exists before creating it
        if (exists(path)) {
            return;
        }

        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder();
        // perform 1 put request per parent directory in the path
        for (int i = 0; i < directories.length; i++) {
            aggregatedPath.append(directories[i]).append("/");
            try {
                this.minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(aggregatedPath.toString())
                    .stream(new ByteArrayInputStream(new byte[0]), 0, 0)
                    .contentType("application/x-directory")
                    .build()
                );
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean delete(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }
        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            return !deleteByPrefix(tenantId, namespace, uri.getPath().endsWith("/") ? uri : URI.create(uri + "/")).isEmpty();
        }

        try {
            this.minioClient.removeObject(
                RemoveObjectArgs.builder()
                    .bucket(bucket)
                    .object(getPath(tenantId, uri))
                    .build()
            );

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.endsWith("/")) {
            // Directory are just objects ending with a /
            path = path + "/";
        }
        mkdirs(path);

        try {
            this.minioClient.putObject(PutObjectArgs.builder()
                .bucket(bucket)
                .object(path)
                .stream(new ByteArrayInputStream(new byte[]{}), 0, -1)
                .build()
            );
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }

        return URI.create(getPath("kestra://", uri));
    }

    @Override
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);
        List<DeleteObject> toDelete = new ArrayList<>();

        try {
            FileAttributes attributes = getAttributes(tenantId, namespace, from);
            if (attributes.getType() == FileAttributes.FileType.Directory) {
                String sourcePrefix = toPrefix(source, true);
                Iterable<Result<Item>> results = this.minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(sourcePrefix)
                    .delimiter("/")
                    .recursive(true)
                    .build());
                for (Result<Item> result : results) {
                    Item item = result.get();
                    String newKey = dest + "/" + item.objectName().substring(sourcePrefix.length());
                    move(item.objectName(), newKey, toDelete);
                }
            } else {
                move(source, dest, toDelete);
            }
            Iterable<Result<DeleteError>> results = this.minioClient.removeObjects(RemoveObjectsArgs.builder()
                .bucket(bucket)
                .objects(toDelete)
                .build());
            for (Result<DeleteError> result : results) {
                DeleteError deleteError = result.get();
                if (deleteError != null) {
                    throw new IOException(deleteError.message());
                }
            }
        } catch (MinioException e) {
            throw reThrowMinioStorageException(from.toString(), e);
        } catch (FileNotFoundException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        return URI.create(getPath("kestra://", to));
    }

    private void move(String source, String dest, List<DeleteObject> toDelete) throws Exception {
        mkdirs(dest);
        this.minioClient.copyObject(CopyObjectArgs.builder()
            .bucket(bucket)
            .object(dest)
            .source(CopySource.builder()
                .bucket(bucket)
                .object(source)
                .build())
            .build());
        toDelete.add(new DeleteObject(source));
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        List<Pair<String, DeleteObject>> deleted = Streams
            .stream(this.minioClient
                .listObjects(ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(toPrefix(getPath(tenantId, storagePrefix), false))
                    .recursive(true)
                    .build()
                )
            )
            .map(throwFunction(itemResult -> {
                try {
                    return Pair.of(itemResult.get().objectName(), new DeleteObject(itemResult.get().objectName()));
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }))
            .toList();

        Iterable<Result<DeleteError>> results = this.minioClient.removeObjects(RemoveObjectsArgs.builder()
            .bucket(bucket)
            .objects(deleted.stream().map(Pair::getRight).toList())
            .build()
        );

        if (results.iterator().hasNext()) {
            throw new IOException("Unable to delete all files, failed on [" +
                Streams
                    .stream(results)
                    .map(throwFunction(r -> {
                        try {
                            return r.get().objectName();
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                    }))
                    .collect(Collectors.joining(", ")) +
                "]");
        }

        return deleted
            .stream()
            .map(Pair::getLeft)
            .map(name -> name.replace(tenantId + "/", ""))
            .map(name -> name.endsWith("/") ? name.substring(0, name.length() - 1) : name)
            .map(name -> URI.create("kestra:///" + name))
            .collect(Collectors.toList());
    }

    private String toPrefix(String path, boolean isDirectory) {
        boolean isRoot = path.isEmpty();
        if (isDirectory && !isRoot) {
            return path.endsWith("/") ? path : path + "/";
        }

        return path;
    }

    @NotNull
    private String getPath(String tenantId, URI uri) {
        parentTraversalGuard(uri);
        String path = Optional.ofNullable(uri).map(URI::getPath).orElse("");
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (tenantId == null) {
            return path;
        }
        return tenantId + "/" + path;
    }

    private IOException reThrowMinioStorageException(String uri, MinioException e) {
        if (e instanceof ErrorResponseException && ((ErrorResponseException) e).errorResponse().code().equals("NoSuchKey")) {
            return new FileNotFoundException(uri + " (File not found)");
        }
        return new IOException(e);
    }

    @VisibleForTesting
    MinioClient minioClient() {
        return minioClient;
    }

    @Override
    public void close() {
        if (this.minioClient != null) {
            try {
                this.minioClient.close();
            } catch (Exception e) {
                LOG.warn("Failed to close GcsStorage", e);
            }
        }
    }
}
