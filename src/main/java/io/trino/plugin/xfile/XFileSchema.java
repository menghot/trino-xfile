package io.trino.plugin.xfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class XFileSchema {
    private final String name;
    private final Map<String, Object> properties;
    private List<XFileTable> tables;

    @JsonCreator
    public XFileSchema(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, Object> properties,
            @JsonProperty("tables") List<XFileTable> tables) {
        this.name = name;
        this.properties = properties;
        this.tables = tables;
    }

    @JsonProperty
    public List<XFileTable> getTables() {
        return tables;
    }

    public void setTables(List<XFileTable> tables) {
        this.tables = tables;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Map<String, Object> getProperties() {
        return properties;
    }
}
