package io.trino.plugin.example;

import java.util.Map;
import java.util.Set;

public interface IExampleClient {
    Set<ExampleSchema> getSchemas();

    ExampleSchema getSchema(String name);

    Set<String> getSchemaNames();

    Set<String> getTableNames(String schema);

    ExampleTable getTable(String schema, String tableName);
}
