package io.trino.plugin.xfile;

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

public class XFileTableProperties {
    public static final String FORMAT_PROPERTY = "format";
    public static final String LOCATION_PROPERTY = "location";
    private final List<PropertyMetadata<?>> tableProperties;

    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()

                .add(stringProperty(
                        FORMAT_PROPERTY,
                        "file format",
                        null,
                        false))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "Fil location",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }
}
