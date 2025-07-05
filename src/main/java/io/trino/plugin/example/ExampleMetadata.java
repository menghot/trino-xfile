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
package io.trino.plugin.example;

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
import io.trino.plugin.example.parquet.TrinoParquetFileDataSource;
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
import static io.trino.plugin.example.parquet.ParquetTypeUtils.convertParquetTypeToTrino;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class ExampleMetadata
        implements ConnectorMetadata {
    private final ExampleClient exampleClient;
    private final ExampleFileSystemFactory exampleFileSystemFactory;

    @Inject
    public ExampleMetadata(ExampleClient exampleClient, ExampleFileSystemFactory exampleFileSystemFactory) {
        this.exampleClient = requireNonNull(exampleClient, "exampleClient is null");
        this.exampleFileSystemFactory = requireNonNull(exampleFileSystemFactory, "exampleFileSystemFactory is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames() {
        return ImmutableList.copyOf(exampleClient.getSchemaNames());
    }

    @Override
    public ExampleTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {

        //System.out.println(tableName);
        if (tableName.getTableName().endsWith(".parquet")) {
            //
            return new ExampleTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }

        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ExampleTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {

        SchemaTableName tableName = ((ExampleTableHandle) tableHandle).toSchemaTableName();
        TrinoFileSystem trinoFileSystem = exampleFileSystemFactory.create(session.getIdentity(), Map.of());

        if (tableName.getTableName().endsWith(".parquet")) {
           TrinoInputFile trinoInputFile =  trinoFileSystem.newInputFile(Location.of(tableName.getTableName()));
            try {
                ParquetDataSource dataSource = new TrinoParquetFileDataSource(trinoInputFile);
                return getConnectorTableMetadata(dataSource, tableName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
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
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        ExampleTableHandle exampleTableHandle = (ExampleTableHandle) handle;
        if (constraint.getSummary().getDomains().isPresent()) {
            constraint.getSummary().getDomains().get().forEach((ch, domain) -> {
                ExampleColumnHandle columnHandle = (ExampleColumnHandle) ch;
                if (domain.isSingleValue()) {
                    if (domain.getSingleValue() instanceof Slice s) {
                        exampleTableHandle.getFilterMap().putIfAbsent(columnHandle.getColumnName(), s.toStringUtf8());

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
                    exampleTableHandle.getFilterMap().putIfAbsent(columnHandle.getColumnName(), values);
                }
            });
        }


        return ConnectorMetadata.super.applyFilter(session, exampleTableHandle, constraint);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {

        if (optionalSchemaName.isPresent() && exampleClient.getSchema(optionalSchemaName.get()) != null) {
            String path = exampleClient.getSchema(optionalSchemaName.get()).getProperties().get("auto_path");
            if(path != null) {
                // Auto discovery file as table
                TrinoFileSystem trinoFileSystem = exampleFileSystemFactory.create(session.getIdentity(), Map.of());
                try {
                    FileIterator fileIterator = trinoFileSystem.listFiles(Location.of(path));
                    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
                    while (fileIterator.hasNext()) {
                        FileEntry fileEntry = fileIterator.next();
                        if (fileEntry.location().fileName().matches(".*\\.(csv|parquet)$")) {
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
                .orElseGet(() -> ImmutableSet.copyOf(exampleClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : exampleClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ExampleTableHandle exampleTableHandle = (ExampleTableHandle) tableHandle;
        if (exampleTableHandle.getTableName().endsWith(".parquet")) {
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            AtomicInteger index = new AtomicInteger();
            for (ColumnMetadata column : getTableMetadata(session, exampleTableHandle).getColumns()) {
                columnHandles.put(column.getName(), new ExampleColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
            }
            return columnHandles.buildOrThrow();
        }

        ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new ExampleColumnHandle(column.getName(), column.getType(), index.getAndIncrement(), false));
        }

        Arrays.stream(ExampleInternalColumn.values()).iterator().forEachRemaining(column -> {
            columnHandles.put(column.getName(),
                    new ExampleColumnHandle(column.getName(),
                            VarcharType.createUnboundedVarcharType(),
                            index.getAndIncrement(), true));
        });

        return columnHandles.buildOrThrow();
    }

//    @Override
//    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
//        requireNonNull(prefix, "prefix is null");
//        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
//        for (SchemaTableName tableName : listTables(session, prefix)) {
//            ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
//            if (table == null) {
//                return null;
//            }
//            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
//            columns.put(tableName, tableMetadata.getColumns());
//        }
//        return columns.buildOrThrow();
//    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((ExampleColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return ConnectorMetadata.super.getTableStatistics(session, tableHandle);
    }
}
