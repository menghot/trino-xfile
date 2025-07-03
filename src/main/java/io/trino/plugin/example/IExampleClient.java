package io.trino.plugin.example;

import java.util.Set;

public interface IExampleClient {
    Set<String> getSchemaNames();

    Set<String> getTableNames(String schema);

    ExampleTable getTable(String schema, String tableName);
}
