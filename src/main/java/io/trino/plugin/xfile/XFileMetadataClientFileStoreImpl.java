/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.xfile;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.*;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.TrinoPrincipal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class XFileMetadataClientFileStoreImpl implements XFileMetadataClient {

    private final XFileConfig config;
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final JsonCodec<XFileCatalog> jsonCodec;
    private volatile XFileCatalog xFileCatalog;


    @Override
    public XFileCatalog getXFileCatalog(ConnectorSession session) {
        TrinoFileSystem fileSystem = trinoFileSystemFactory.create(session);
        try (TrinoInputStream inputStream = fileSystem.newInputFile(Location.of(config.getMetadataLocation())).newStream()) {
            xFileCatalog = jsonCodec.fromJson(inputStream);
        } catch (IOException e) {
            throw new TrinoException(StandardErrorCode.CONFIGURATION_INVALID, e);
        }
        return xFileCatalog;
    }

    @Inject
    public XFileMetadataClientFileStoreImpl(
            XFileConfig config,
            TrinoFileSystemFactory trinoFileSystemFactory,
            JsonCodec<XFileCatalog> jsonCodec) {
        this.config = config;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.jsonCodec = jsonCodec;
    }


    public List<XFileSchema> getSchemas(ConnectorSession session) {
        getXFileCatalog(session);
        return xFileCatalog.getSchemas();
    }

    public XFileSchema getSchema(ConnectorSession session, String name) {
        getXFileCatalog(session);
        return getSchemas(session).stream().filter(schema -> schema.getName().equals(name)).findFirst().orElse(null);
    }

    @Override
    public Set<String> getSchemaNames(ConnectorSession session) {
        return getSchemas(session).stream().map(XFileSchema::getName).collect(Collectors.toSet());
    }

    @Override
    public XFileTable getTable(ConnectorSession session, String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Optional<XFileSchema> xFileSchema = getSchemas(session).stream().filter(s -> s.getName().equals(schema)).findFirst();
        if (xFileSchema.isPresent() && xFileSchema.get().getTables() != null) {
            return xFileSchema.get().getTables().stream().filter(t -> t.name().equals(tableName)).findFirst().orElse(null);
        }
        return null;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner) {
        getXFileCatalog(session);
        xFileCatalog.getSchemas().add(new XFileSchema(schemaName, properties, List.of()));
        saveCatalog(session, xFileCatalog);
    }


    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
        getXFileCatalog(session);
        XFileSchema xFileSchema = getSchema(session, tableMetadata.getTable().schemaName());
        List<XFileColumn> columns = tableMetadata.getColumns().stream().map(
                c -> new XFileColumn(c.getName(), c.getType())).toList();

        if (xFileSchema.getProperties().containsKey(XFileConnector.TABLE_PROP_FILE_LOCATION)) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "The schema is used for auto discovery, Can't create table in schema: " + tableMetadata.getTable().schemaName());
        }


        if(!tableMetadata.getTable().tableName().matches(XFileConnector.FILE_FILTER_REGEX)) {
            if (!tableMetadata.getProperties().containsKey(XFileConnector.TABLE_PROP_FILE_FORMAT)) {
                throw new TrinoException(StandardErrorCode.INVALID_TABLE_PROPERTY, "Table property must contain: '"
                        + XFileConnector.TABLE_PROP_FILE_FORMAT + "' for table: " + tableMetadata.getTable().tableName());
            }

            if (!tableMetadata.getProperties().containsKey(XFileConnector.TABLE_PROP_FILE_FILTER_REGEX)) {
                throw new TrinoException(StandardErrorCode.INVALID_TABLE_PROPERTY, "Table property must contain: '"
                        + XFileConnector.TABLE_PROP_FILE_FILTER_REGEX + "' for table: " + tableMetadata.getTable().tableName());
            }
        }

        xFileSchema.getTables().add(
                new XFileTable(
                        tableMetadata.getTable().tableName(),
                        columns,
                        tableMetadata.getProperties())
        );

        saveCatalog(session, xFileCatalog);
    }

    private void saveCatalog(ConnectorSession session, XFileCatalog xFileCatalog) {
        TrinoFileSystem fileSystem = trinoFileSystemFactory.create(session);
        TrinoOutputFile trinoOutputFile = fileSystem.newOutputFile(Location.of(config.getMetadataLocation()));
        try {
            String json = jsonCodec.toJson(xFileCatalog);
            trinoOutputFile.createOrOverwrite(json.getBytes());
        } catch (IOException e) {
            throw new TrinoException(StandardErrorCode.CONFIGURATION_INVALID, e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        getXFileCatalog(session);
        xFileCatalog.setSchemas(xFileCatalog.getSchemas().stream()
                .filter(s -> !s.getName().equals(schemaName)).toList());

        saveCatalog(session, xFileCatalog);
    }

    @Override
    public void dropTable(ConnectorSession session, String schemaName, String tableName) {
        getXFileCatalog(session);
        XFileSchema xFileSchema = getSchema(session, schemaName);
        xFileSchema.setTables(
                xFileSchema.getTables().stream().filter(
                        t -> !t.name().equals(tableName)).toList());
        saveCatalog(session, xFileCatalog);
    }

    @Override
    public FileIterator listFiles(ConnectorSession session, String path) {
        TrinoFileSystem fileSystem = trinoFileSystemFactory.create(session);
        try {
            String filePath = path.replaceAll("#\\d+$","");
            return fileSystem.listFiles(Location.of(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
