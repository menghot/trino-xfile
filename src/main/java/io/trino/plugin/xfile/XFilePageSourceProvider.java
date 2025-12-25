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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.xfile.parquet.ParquetFileDataSource;
import io.trino.plugin.xfile.parquet.ParquetPageDataSource;
import io.trino.plugin.xfile.record.XFileRecordSetProvider;
import io.trino.spi.connector.*;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.xfile.parquet.ParquetReaderUtils.createParquetReader;

public class XFilePageSourceProvider
        implements ConnectorPageSourceProvider {

    private final ConnectorRecordSetProvider recordSetProvider;
    TrinoFileSystemFactory trinoFileSystemFactory;

    public XFilePageSourceProvider(
            TrinoFileSystemFactory trinoFileSystemFactory) {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.recordSetProvider = new XFileRecordSetProvider(trinoFileSystemFactory);
    }

    private static ParquetPageDataSource getParquetPageSource(List<ColumnHandle> columns, ParquetDataSource dataSource) {

        List<String> columnNames = columns.stream().map((columnHandle) -> {
            XFileColumnHandle c = (XFileColumnHandle) columnHandle;
            return c.getColumnName();
        }).toList();

        List<Type> types = columns.stream().map((columnHandle) -> {
            XFileColumnHandle c = (XFileColumnHandle) columnHandle;
            return c.getColumnType();
        }).toList();

        try {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            ParquetReader reader = createParquetReader(dataSource, parquetMetadata, types, columnNames);
            return new ParquetPageDataSource(reader);
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

        XFileSplit xFileSplit = (XFileSplit) split;
        XFileTableHandle tableHandle = (XFileTableHandle) table;

        // 1. Parquet
        if ("parquet".equals(xFileSplit.properties().get(XFileConnector.TABLE_PROP_FILE_FORMAT)) ||
                xFileSplit.uri().endsWith(".parquet")) {
            TrinoInputFile trinoInputFile = trinoFileSystemFactory.create(session).newInputFile(Location.of(xFileSplit.uri()));
            try {
                return getParquetPageSource(columns, new ParquetFileDataSource(trinoInputFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (
                "csv".equals(xFileSplit.properties().get(XFileConnector.TABLE_PROP_FILE_FORMAT)) ||
                        xFileSplit.uri().matches(XFileConnector.FILE_TABLE_CSV_REGEX) ||
                        xFileSplit.uri().endsWith(".json")) {
            return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, xFileSplit, tableHandle, columns));
        }

        return null;
    }
}
