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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.filesystem.*;
import io.trino.plugin.xfile.utils.XFileTableMetadataUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class XFileMetadata
        implements ConnectorMetadata {
    private final XFileClientSimple xFileClient;
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public XFileMetadata(XFileClientSimple xFileClient, TrinoFileSystemFactory trinoFileSystemFactory) {
        this.xFileClient = requireNonNull(xFileClient, "XFileClientSimple is null");
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "exampleFileSystemFactory is null");
    }


    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames() {
        return ImmutableList.copyOf(xFileClient.getSchemaNames());
    }

    @Override
    public XFileTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {

        if (tableName.getTableName().matches(XFileConstants.FILE_TABLE_REGEX)) {
            // Table name starts s3:// or local:// and ends with .csv / .parquet
            return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }

        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        XFileTable table = xFileClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {

        SchemaTableName schemaTableName = ((XFileTableHandle) tableHandle).toSchemaTableName();
        // 1. Get table metadata from parquet/csv file
        if (schemaTableName.getTableName().matches(XFileConstants.FILE_TABLE_REGEX)) {
            TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
            if (schemaTableName.getTableName().endsWith(".parquet")) {
                return XFileTableMetadataUtils.getParquetConnectorTableMetadata(trinoFileSystem, schemaTableName);
            }
            return XFileTableMetadataUtils.getCsvConnectorTableMetadata(trinoFileSystem, schemaTableName);
        } else {
            XFileTable table = xFileClient.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table == null) {
                return null;
            }
            // Get metadata from metadata api
            return new ConnectorTableMetadata(schemaTableName, table.getColumnsMetadata());
        }
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

        // If schema not preset, retrieve all schemas
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(xFileClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            XFileSchema xFileSchema = xFileClient.getSchema(schemaName);
            Object location = xFileSchema.getProperties().get("location");
            if (location != null) {
                // Auto discovery files as tables
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
                for (String tableName : xFileClient.getTableNames(schemaName)) {
                    builder.add(new SchemaTableName(schemaName, tableName));
                }
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        XFileTableHandle xFileTableHandle = (XFileTableHandle) tableHandle;
        if (xFileTableHandle.getTableName().matches(XFileConstants.FILE_TABLE_REGEX)) {

            if (xFileTableHandle.getTableName().endsWith(".parquet")) {
                ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
                AtomicInteger index = new AtomicInteger();
                for (ColumnMetadata column : getTableMetadata(session, xFileTableHandle).getColumns()) {
                    columnHandles.put(column.getName(), new XFileColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
                }
                return columnHandles.buildOrThrow();
            }

            return XFileTableMetadataUtils.getCsvFileColumnHandles(trinoFileSystemFactory.create(session), xFileTableHandle.getTableName());
        }

        XFileTable table = xFileClient.getTable(xFileTableHandle.getSchemaName(), xFileTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(xFileTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new XFileColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
        }

        Arrays.stream(XFileInternalColumn.values()).iterator().forEachRemaining(column -> columnHandles.put(column.getName(),
                new XFileColumnHandle(column.getName(),
                        VarcharType.createUnboundedVarcharType(),
                        index.getAndIncrement(), true)));

        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((XFileColumnHandle) columnHandle).getColumnMetadata();
    }


    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner) {
        xFileClient.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
        xFileClient.createTable(session, tableMetadata, saveMode);
    }
}
