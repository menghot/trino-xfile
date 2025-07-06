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
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.xfile.parquet.XFileParquetFileDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.plugin.xfile.utils.ParquetTypeUtils.convertParquetTypeToTrino;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class XFileMetadata
        implements ConnectorMetadata {
    private final XFileClient XFileClient;
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public XFileMetadata(XFileClient XFileClient, TrinoFileSystemFactory trinoFileSystemFactory) {
        this.XFileClient = requireNonNull(XFileClient, "XFileClient is null");
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "exampleFileSystemFactory is null");
    }

    private static ConnectorTableMetadata getConnectorTableMetadata(ParquetDataSource dataSource, SchemaTableName tableName) throws IOException {
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        System.out.println(parquetMetadata);

        FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();

        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
        System.out.println(messageColumnIO);

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            String name = field.getName();
            Type trinoType = convertParquetTypeToTrino(field);
            columnsMetadata.add(new ColumnMetadata(name, trinoType));
        }
        return new ConnectorTableMetadata(tableName, columnsMetadata.build());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames() {
        return ImmutableList.copyOf(XFileClient.getSchemaNames());
    }

    @Override
    public XFileTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {

        //System.out.println(tableName);
        if (tableName.getTableName().endsWith("(local|s3).*\\.(csv|parquet)$")) {
            //
            return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }

        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        XFileTable table = XFileClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new XFileTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {

        SchemaTableName tableName = ((XFileTableHandle) tableHandle).toSchemaTableName();
        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);

        if (tableName.getTableName().endsWith(".parquet")) {
            TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(Location.of(tableName.getTableName()));
            try {
                ParquetDataSource dataSource = new XFileParquetFileDataSource(trinoInputFile);
                return getConnectorTableMetadata(dataSource, tableName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        XFileTable table = XFileClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
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

        if (optionalSchemaName.isPresent() && XFileClient.getSchema(optionalSchemaName.get()) != null) {
            String path = XFileClient.getSchema(optionalSchemaName.get()).getProperties().get("auto_path");
            if (path != null) {
                // Auto discovery file as table
                TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
                try {
                    FileIterator fileIterator = trinoFileSystem.listFiles(Location.of(path));
                    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
                    while (fileIterator.hasNext()) {
                        FileEntry fileEntry = fileIterator.next();
                        if (fileEntry.location().fileName().matches("(local|s3).*\\.(csv|parquet)$")) {
                            builder.add(new SchemaTableName(optionalSchemaName.get(), fileEntry.location().toString()));
                        }
                    }
                    return builder.build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(XFileClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : XFileClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        XFileTableHandle XFileTableHandle = (XFileTableHandle) tableHandle;
        if (XFileTableHandle.getTableName().endsWith(".parquet")) {
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            AtomicInteger index = new AtomicInteger();
            for (ColumnMetadata column : getTableMetadata(session, XFileTableHandle).getColumns()) {
                columnHandles.put(column.getName(), new XFileColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
            }
            return columnHandles.buildOrThrow();
        }

        XFileTable table = XFileClient.getTable(XFileTableHandle.getSchemaName(), XFileTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(XFileTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new XFileColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
        }

        Arrays.stream(XFileInternalColumn.values()).iterator().forEachRemaining(column -> {
            columnHandles.put(column.getName(),
                    new XFileColumnHandle(column.getName(),
                            VarcharType.createUnboundedVarcharType(),
                            index.getAndIncrement(), true));
        });

        return columnHandles.buildOrThrow();
    }


    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((XFileColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return ConnectorMetadata.super.getTableStatistics(session, tableHandle);
    }
}
