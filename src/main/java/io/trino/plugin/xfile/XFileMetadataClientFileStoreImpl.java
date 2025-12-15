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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class XFileMetadataClientFileStoreImpl implements XFileMetadataClient {

    private final List<XFileSchema> schemas;
    private TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public XFileMetadataClientFileStoreImpl(
            XFileConfig config,
            TrinoFileSystemFactory trinoFileSystemFactory,
            JsonCodec<XFileCatalog> jsonCodec) {

        TrinoFileSystem fileSystem = trinoFileSystemFactory.create(ConnectorIdentity.ofUser("admin"));
        try (TrinoInputStream inputStream = fileSystem.newInputFile(Location.of("s3://metastore/example-metadata-http.json")).newStream()) {
            XFileCatalog catalog = jsonCodec.fromJson(inputStream);
            schemas = catalog.getSchemas();
            System.out.println(catalog);
        } catch (Exception e) {
            throw new TrinoException(StandardErrorCode.CONFIGURATION_INVALID, e);
        }
    }


    public List<XFileSchema> getSchemas() {
        return schemas;
    }

    //============================================================

    public XFileSchema getSchema(String name) {
        return getSchemas().stream().filter(schema -> schema.getName().equals(name)).findFirst().orElse(null);
    }

    @Override
    public Set<String> getSchemaNames() {
        return schemas.stream().map(XFileSchema::getName).collect(Collectors.toSet());
    }

    @Override
    public XFileTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Optional<XFileSchema> xFileSchema = schemas.stream().filter(s -> s.getName().equals(schema)).findFirst();
        if (xFileSchema.isPresent() && xFileSchema.get().getTables() != null) {
            return xFileSchema.get().getTables().stream().filter(t -> t.getName().equals(tableName)).findFirst().orElse(null);
        }
        return null;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner) {
        schemas.add(new XFileSchema(schemaName, properties, List.of()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {

        XFileSchema xFileSchema = getSchema(tableMetadata.getTable().getSchemaName());

        List<XFileColumn> columns = tableMetadata.getColumns().stream().map(c -> new XFileColumn(
                c.getName(),
                c.getType()
        )).toList();


        xFileSchema.getTables().add(new
                XFileTable(
                tableMetadata.getTable().getTableName(),
                columns,
                tableMetadata.getProperties())
        );
    }
}
