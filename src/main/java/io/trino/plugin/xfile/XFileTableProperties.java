package io.trino.plugin.xfile;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

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
