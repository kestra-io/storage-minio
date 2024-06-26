package io.kestra.storage.minio.internal;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Locale;
import java.util.OptionalLong;

/**
 * Wrapper for long representation of a byte size.
 *
 * @param value a string bytes size.
 */
public record BytesSize(long value) {

    private static final String KILOBYTES = "KB";
    private static final String MEGABYTES = "MB";
    private static final String GIGABYTES = "GB";
    private static final int KB_UNIT = 1024;

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public BytesSize {}

    /**
     * Constructor for a new {@link BytesSize} object from a given string.
     *
     * @param value a human-readable bytes size, e.g. 1KB, 1MB, 1GB.
     */
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public BytesSize(final String value) {
        this(convert(value).orElseThrow(
            () -> new IllegalArgumentException(String.format("Cannot convert '%s' to long bytes size.", value))));
    }

    private static OptionalLong convert(final String object) {
        if (object == null || object.isEmpty()) {
            return OptionalLong.empty();
        }
        final String value = object.toUpperCase(Locale.ENGLISH);
        try {
            long size;
            if (value.endsWith(KILOBYTES)) {
                long numberPart = parseSizeWithUnit(value);
                size = numberPart * KB_UNIT;
            } else if (value.endsWith(MEGABYTES)) {
                long numberPart = parseSizeWithUnit(value);
                size = numberPart * KB_UNIT * KB_UNIT;
            } else if (value.endsWith(GIGABYTES)) {
                long numberPart = parseSizeWithUnit(value);
                size = numberPart * KB_UNIT * KB_UNIT * KB_UNIT;
            } else {
                size = Long.parseLong(value);
            }
            return OptionalLong.of(size);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    private static long parseSizeWithUnit(final String value) {
        return Long.parseLong(value.substring(0, value.length() - 2));
    }
}
