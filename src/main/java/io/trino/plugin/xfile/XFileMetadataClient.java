package io.trino.plugin.xfile;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface XFileMetadataClient {

    XFileCatalog getXFileCatalog(ConnectorSession session);

    List<XFileSchema> getSchemas(ConnectorSession session);

    XFileSchema getSchema(ConnectorSession session, String name);

    Set<String> getSchemaNames(ConnectorSession session);

    XFileTable getTable(ConnectorSession session, String schema, String tableName);

    void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode);

    void dropSchema(ConnectorSession session, String schemaName);

    void dropTable(ConnectorSession session, String schemaName, String tableName);
}
