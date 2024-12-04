package io.kestra.storage.minio;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MetadataUtils {
    private static final Pattern METADATA_KEY_WORD_SEPARATOR = Pattern.compile("_([a-z])");
    private static final Pattern UPPERCASE = Pattern.compile("([A-Z])");

    public static Map<String, String> toStoredMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        return metadata.entrySet().stream()
            .map(entry -> Map.entry(UPPERCASE.matcher(entry.getKey()).replaceAll("_$1").toLowerCase(), entry.getValue()))
            .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
    }

    public static Map<String, String> toRetrievedMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        return metadata.entrySet().stream()
            .map(entry -> Map.entry(
                METADATA_KEY_WORD_SEPARATOR.matcher(entry.getKey())
                    .replaceAll(matchResult -> matchResult.group(1).toUpperCase()),
                entry.getValue()
            )).collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
    }
}
