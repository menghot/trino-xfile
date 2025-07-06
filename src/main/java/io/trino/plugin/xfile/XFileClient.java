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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class XFileClient implements IXFileClient {
    /**
     * SchemaName -> (TableName -> XFileTable)
     */
    private final Supplier<Map<String, Map<String, XFileTable>>> schemasSupplier;


    @Inject
    public XFileClient(
            XFileConfig config,
            JsonCodec<Map<String,
                    List<XFileTable>>> exampleTableList
    ) {
        requireNonNull(exampleTableList, "exampleTableList is null");
        schemasSupplier = Suppliers.memoize(schemasSupplier(exampleTableList, config.getMetadataUri()));
    }

    private static Supplier<Map<String, Map<String, XFileTable>>> schemasSupplier(JsonCodec<Map<String, List<XFileTable>>> catalogCodec, URI metadataUri) {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, XFileTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<XFileTable>>> catalogCodec)
            throws IOException {
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        Map<String, List<XFileTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<XFileTable>, Map<String, XFileTable>> resolveAndIndexTables(URI metadataUri) {
        return tables -> {
            Iterable<XFileTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, XFileTable::getName));
        };
    }

    private static Function<XFileTable, XFileTable> tableUriResolver(URI baseUri) {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new XFileTable(table.getName(), table.getColumns(), sources, null);
        };
    }

    public Set<XFileSchema> getSchemas() {
        return Set.of(
                new XFileSchema("s3", Map.of("auto_path", "s3://hive/warehouse/ods.db")),
                new XFileSchema("local", Map.of("auto_path", "local:///")),
                new XFileSchema("example", Map.of()),
                new XFileSchema("tpch", Map.of()));
    }

    //============================================================

    public XFileSchema getSchema(String name) {
        return getSchemas().stream().filter(schema -> schema.getName().equals(name)).findFirst().orElse(null);
    }

    @Override
    public Set<String> getSchemaNames() {
        return requireNonNull(schemasSupplier.get()).keySet();
    }

    @Override
    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, XFileTable> tables = requireNonNull(schemasSupplier.get()).get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    @Override
    public XFileTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, XFileTable> tables = schemasSupplier.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }
}
