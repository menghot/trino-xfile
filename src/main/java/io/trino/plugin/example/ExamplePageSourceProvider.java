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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.example.parquet.ParquetPageSource;
import io.trino.plugin.example.parquet.TrinoParquetFileDataSource;
import io.trino.plugin.example.record.ExampleRecordSetProvider;
import io.trino.spi.connector.*;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.example.utils.ParquetUtils.createParquetReader;

public class ExamplePageSourceProvider
        implements ConnectorPageSourceProvider {

    private final ConnectorRecordSetProvider recordSetProvider;
    TrinoFileSystemFactory trinoFileSystemFactory;

    public ExamplePageSourceProvider(
            TrinoFileSystemFactory trinoFileSystemFactory
    ) {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.recordSetProvider = new ExampleRecordSetProvider(trinoFileSystemFactory);
    }

    private static ParquetPageSource getParquetPageSource(List<ColumnHandle> columns, ParquetDataSource dataSource) {

        List<String> columnNames = columns.stream().map((columnHandle) -> {
            ExampleColumnHandle c = (ExampleColumnHandle) columnHandle;
            return c.getColumnName();
        }).toList();

        List<Type> types = columns.stream().map((columnHandle) -> {
            ExampleColumnHandle c = (ExampleColumnHandle) columnHandle;
            return c.getColumnType();
        }).toList();

        try {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames);
            return new ParquetPageSource(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {

        ExampleSplit exampleSplit = (ExampleSplit) split;
        ExampleTableHandle tableHandle = (ExampleTableHandle) table;
        if (tableHandle.getTableName().endsWith(".parquet")) {
            TrinoInputFile trinoInputFile = trinoFileSystemFactory.create(session).newInputFile(Location.of(tableHandle.getTableName()));
            try {
                return getParquetPageSource(columns, new TrinoParquetFileDataSource(trinoInputFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, exampleSplit, tableHandle, columns));
    }
}
