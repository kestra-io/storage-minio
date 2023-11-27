package io.kestra.storage.minio;

import com.google.common.collect.Streams;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@MinioStorageEnabled
@Introspected
public class MinioStorage implements StorageInterface {
    @Inject
    MinioClient minioClient;

    @Inject
    MinioConfig config;

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        try {
            return this.minioClient.getObject(GetObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(getPath(tenantId, uri))
                .build()
            );
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> filesByPrefix(String tenantId, URI prefix) throws IOException {
        String internalStoragePrefix = getPath(tenantId, prefix);
        String prefixForMinio = toPrefix(internalStoragePrefix, false);
        return keysForPrefix(prefixForMinio, true)
            .filter(name -> !name.endsWith("/"))
            .map(name -> URI.create("kestra://" + prefix.getPath() + name.substring(internalStoragePrefix.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, URI uri) throws IOException {
        try {
            String prefix = toPrefix(getPath(tenantId, uri), true);
            List<FileAttributes> list = keysForPrefix(prefix, false)
                .map(throwFunction(this::getFileAttributes))
                .toList();
            if (list.isEmpty()) {
                // this will throw FileNotFound if there is no directory
                this.getAttributes(tenantId, uri);
            }
            return list;
        } catch (FileNotFoundException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Stream<String> keysForPrefix(String prefix, boolean recursive) throws IOException {
        try {
            Iterable<Result<Item>> results = this.minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(config.getBucket())
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
                        && (recursive || Path.of(name).getParent() == null);
                })
                .map(name -> name.startsWith("/") ? name : "/" + name);
        } catch (MinioException e) {
            throw reThrowMinioStorageException(prefix, e);
        } catch (FileNotFoundException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    @Override
    public boolean exists(String tenantId, URI uri) {
        // There is no way to check if an object exist so we gather the stat of the object which will throw an exception
        // if the object didn't exist.
        String path = getPath(tenantId, uri);
        try {
            this.minioClient.statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(path)
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
            return this.minioClient.statObject(StatObjectArgs.builder()
                    .bucket(this.config.getBucket())
                    .object(getPath(tenantId, uri))
                    .build()
                )
                .size();
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Long lastModifiedTime(String tenantId, URI uri) throws IOException {
        try {
            return this.minioClient.statObject(StatObjectArgs.builder()
                    .bucket(this.config.getBucket())
                    .object(getPath(tenantId, uri))
                    .build()
                )
                .lastModified().toInstant().toEpochMilli();
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.endsWith("/") && !exists(tenantId, uri)) {
            // if key does not exist we try to get the "directory" (directory are just object ending with /)
            path = path + "/";
        }
        return getFileAttributes(path);
    }

    private FileAttributes getFileAttributes(String path) throws IOException {
        try {
            StatObjectResponse stat = this.minioClient.statObject(StatObjectArgs.builder()
                .bucket(this.config.getBucket())
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
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        String path = getPath(tenantId, uri);
        mkdirs(path);
        try {
            this.minioClient.putObject(PutObjectArgs.builder()
                .bucket(this.config.getBucket())
                .object(path)
                .stream(data, -1, config.getPartSize())
                .build()
            );

            data.close();
        } catch (MinioException e) {
            throw reThrowMinioStorageException(uri.toString(), e);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IOException(e);
        }

        return URI.create("kestra://" + uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        path = path.replaceAll("^/*", "");
        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder("/");
        // perform 1 put request per parent directory in the path
        for (int i = 0; i <= directories.length - (path.endsWith("/") ? 1 : 2); i++) {
            aggregatedPath.append(directories[i]).append("/");
            try {
                this.minioClient.putObject(PutObjectArgs.builder()
                    .bucket(this.config.getBucket())
                    .object(aggregatedPath.toString())
                    .stream(new ByteArrayInputStream(new byte[]{}), 0, -1)
                    .build()
                );
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean delete(String tenantId, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, uri);
        } catch (FileNotFoundException e) {
            return false;
        }
        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            return !deleteByPrefix(tenantId, uri.getPath().endsWith("/") ? uri : URI.create(uri + "/")).isEmpty();
        }

        try {
            this.minioClient.removeObject(
                RemoveObjectArgs.builder()
                    .bucket(config.getBucket())
                    .object(getPath(tenantId, uri))
                    .build()
            );

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public URI createDirectory(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.endsWith("/")) {
            // Directory are just objects ending with a /
            path = path + "/";
        }
        mkdirs(path);

        try {
            this.minioClient.putObject(PutObjectArgs.builder()
                .bucket(this.config.getBucket())
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
    public URI move(String tenantId, URI from, URI to) throws IOException {
        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);
        List<DeleteObject> toDelete = new ArrayList<>();

        try {
            FileAttributes attributes = getAttributes(tenantId, from);
            if (attributes.getType() == FileAttributes.FileType.Directory) {
                String sourcePrefix = toPrefix(source, true);
                Iterable<Result<Item>> results = this.minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(config.getBucket())
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
                .bucket(config.getBucket())
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
            .bucket(config.getBucket())
            .object(dest)
            .source(CopySource.builder()
                .bucket(config.getBucket())
                .object(source)
                .build())
            .build());
        toDelete.add(new DeleteObject(source));
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        List<Pair<String, DeleteObject>> deleted = Streams
            .stream(this.minioClient
                .listObjects(ListObjectsArgs.builder()
                    .bucket(this.config.getBucket())
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
            .bucket(this.config.getBucket())
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
        String withoutLeadingSlash = path.substring(1);
        if(isDirectory && !withoutLeadingSlash.isBlank()) {
            return withoutLeadingSlash.endsWith("/") ? withoutLeadingSlash : withoutLeadingSlash + "/";
        }

        return withoutLeadingSlash;
    }

    @NotNull
    private String getPath(String tenantId, URI uri) {
        if (uri == null) {
            uri = URI.create("/");
        }

        parentTraversalGuard(uri);
        String path = uri.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        if (tenantId == null) {
            return path;
        }
        return "/" + tenantId + path;
    }

    private void parentTraversalGuard(URI uri) {
        if (uri.toString().contains("..")) {
            throw new IllegalArgumentException("File should be accessed with their full path and not using relative '..' path.");
        }
    }

    private IOException reThrowMinioStorageException(String uri, MinioException e) {
        if (e instanceof ErrorResponseException && ((ErrorResponseException) e).errorResponse().code().equals("NoSuchKey")) {
            return new FileNotFoundException(uri + " (File not found)");
        }
        return new IOException(e);
    }
}
