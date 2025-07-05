package io.trino.plugin.example;

import java.util.Map;

public class ExampleSchema {
    private final String name;
    private final Map<String, String> properties;


    public ExampleSchema(String name, Map<String, String> properties) {
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
