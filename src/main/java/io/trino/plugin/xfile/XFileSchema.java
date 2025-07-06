package io.trino.plugin.xfile;

import java.util.Map;

public class XFileSchema {
    private final String name;
    private final Map<String, String> properties;


    public XFileSchema(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
