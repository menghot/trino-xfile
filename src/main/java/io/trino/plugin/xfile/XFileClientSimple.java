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

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.TrinoPrincipal;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class XFileClientSimple implements XFileClient {

    private final List<XFileSchema> schemas;

    @Inject
    public XFileClientSimple(
            XFileConfig config,
            JsonCodec<Map<String, List<XFileSchema>>> jsonCodec) {
        try {
            URL result = config.getMetadataUri().toURL();
            String json = Resources.toString(result, UTF_8);
            Map<String, List<XFileSchema>> catalog = jsonCodec.fromJson(json);
            schemas = catalog.get("schemas");
        } catch (Exception e) {
            throw new RuntimeException(e);
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
    public Set<String> getTableNames(String schemaName) {
        return schemas
                .stream().filter(schema -> schema.getName().equalsIgnoreCase(schemaName))
                .findFirst()
                .map(schema -> schema.getTables().stream()
                        .map(XFileTable::getName)
                        .collect(Collectors.toSet())).orElse(Set.of());

    }

    @Override
    public XFileTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Optional<XFileSchema>  xFileSchema=  schemas.stream().filter(s -> s.getName().equals(schema)).findFirst();
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
