package io.trino.plugin.xfile;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface XFileMetadataClient {

    List<XFileSchema> getSchemas();

    XFileSchema getSchema(String name);

    Set<String> getSchemaNames();

    XFileTable getTable(String schema, String tableName);

    void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode);
}
