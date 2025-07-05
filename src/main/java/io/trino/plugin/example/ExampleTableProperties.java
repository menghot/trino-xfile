package io.trino.plugin.example;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public class ExampleTableProperties {
    public static final String FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String LOCATION_PROPERTY = "location";
    public static final String ORC_BLOOM_FILTER_COLUMNS_PROPERTY = "orc_bloom_filter_columns";
    public static final String PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY = "parquet_bloom_filter_columns";
    public static final String DATA_LOCATION_PROPERTY = "data_location";

    private final List<PropertyMetadata<?>> tableProperties;

    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()

                .add(new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition transforms",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Sorted columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        ORC_BLOOM_FILTER_COLUMNS_PROPERTY,
                        "ORC Bloom filter index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(new PropertyMetadata<>(
                        PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY,
                        "Parquet Bloom filter index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(stringProperty(
                        DATA_LOCATION_PROPERTY,
                        "File system location URI for the table's data files",
                        null,
                        false))
                .build();
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties) {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }
}
