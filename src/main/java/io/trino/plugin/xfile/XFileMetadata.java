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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.filesystem.*;
import io.trino.plugin.xfile.utils.XFileTableMetadataUtils;
import io.trino.spi.connector.*;
import io.trino.spi.security.TrinoPrincipal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class XFileMetadata
        implements ConnectorMetadata {
    private final XFileMetadataClient xFileMetadataClient;
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public XFileMetadata(XFileMetadataClient xFileMetadataClient, TrinoFileSystemFactory trinoFileSystemFactory) {
        this.xFileMetadataClient = requireNonNull(xFileMetadataClient, "xFileMetadataClient is null");
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "exampleFileSystemFactory is null");
    }


    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(xFileMetadataClient.getSchemaNames(session));
    }


    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        XFileTableHandle XFileTableHandle = (XFileTableHandle) handle;
        if (constraint.getSummary().getDomains().isPresent()) {
            constraint.getSummary().getDomains().get().forEach((ch, domain) -> {
                XFileColumnHandle columnHandle = (XFileColumnHandle) ch;
                if (domain.isSingleValue()) {
                    if (domain.getSingleValue() instanceof Slice s) {
                        XFileTableHandle.getFilterMap().putIfAbsent(columnHandle.getColumnName(), s.toStringUtf8());
                    }
                } else {
                    List<String> values = new ArrayList<>();
                    domain.getValues().getRanges().getOrderedRanges().iterator().forEachRemaining(r -> {
                        if (r.isSingleValue()) {
                            if (r.getSingleValue() instanceof Slice s) {
                                values.add(s.toStringUtf8());
                            }
                        }
                        // more types support
                    });
                    XFileTableHandle.getFilterMap().putIfAbsent(columnHandle.getColumnName(), values);
                }
            });
        }

        return ConnectorMetadata.super.applyFilter(session, XFileTableHandle, constraint);
    }


    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        if (!optionalSchemaName.isPresent()) {
            return builder.build();
        }

        XFileSchema xFileSchema = xFileMetadataClient.getSchema(session, optionalSchemaName.get());
        Object location = xFileSchema.getProperties().get(XFileConstants.SCHEMA_PROP_LOCATION);
        if (location != null) {
            TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
            try {
                FileIterator fileIterator = trinoFileSystem.listFiles(Location.of(location.toString()));
                while (fileIterator.hasNext()) {
                    FileEntry fileEntry = fileIterator.next();
                    if (fileEntry.location().toString().matches(XFileConstants.FILE_TABLE_REGEX)) {
                        builder.add(new SchemaTableName(optionalSchemaName.get(), fileEntry.location().toString()));
                    }
                }
                return builder.build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            xFileSchema.getTables().forEach(table -> {
                builder.add(new SchemaTableName(optionalSchemaName.get(), table.getName()));
            });
        }

        return builder.build();
    }


    @Override
    public XFileTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {

        XFileTable table = xFileMetadataClient.getTable(session, tableName.getSchemaName(), tableName.getTableName());
        if (table != null) {
            return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }

        XFileSchema xFileSchema = xFileMetadataClient.getSchema(session, tableName.getSchemaName());
        if (xFileSchema == null) {
            return null;
        }

        if (!xFileSchema.getProperties().containsKey(XFileConstants.SCHEMA_PROP_LOCATION)) {
            return null;
        } else if (tableName.getTableName().matches(XFileConstants.FILE_TABLE_REGEX)) {
            return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }

        return null;
    }


    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        SchemaTableName schemaTableName = ((XFileTableHandle) tableHandle).getSchemaTableName();
        XFileTable table = xFileMetadataClient.getTable(session, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (table != null) {
            return new ConnectorTableMetadata(schemaTableName, table.getColumnsMetadata(), table.getProperties());
        } else {
            // Read table metadata from file (parquet, csv...)
            TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
            return XFileTableMetadataUtils.readTableMetadataFromFile(trinoFileSystem, schemaTableName);
        }
    }


    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        XFileTableHandle xFileTableHandle = (XFileTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger();
        getTableMetadata(session, xFileTableHandle).getColumns().forEach(column -> {
            columnHandles.put(column.getName(), new XFileColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
        });
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((XFileColumnHandle) columnHandle).getColumnMetadata();
    }


    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner) {
        xFileMetadataClient.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
        xFileMetadataClient.createTable(session, tableMetadata, saveMode);
    }


    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        XFileTableHandle xFileTableHandle = (XFileTableHandle) tableHandle;
        xFileMetadataClient.dropTable(session, xFileTableHandle.getSchemaName(), xFileTableHandle.getTableName());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
        xFileMetadataClient.dropSchema(session, schemaName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName) {
        return xFileMetadataClient.getSchema(session, schemaName).getProperties();
    }
}
