package io.trino.plugin.xfile;

import java.util.Set;

public interface IXFileClient {
    Set<XFileSchema> getSchemas();

    XFileSchema getSchema(String name);

    Set<String> getSchemaNames();

    Set<String> getTableNames(String schema);

    XFileTable getTable(String schema, String tableName);
}
