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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class XFileSplitManager
        implements ConnectorSplitManager {
    private final XFileMetadataClient xFileMetadataClient;

    @Inject
    public XFileSplitManager(XFileMetadataClient xFileMetadataClient) {
        this.xFileMetadataClient = requireNonNull(xFileMetadataClient, "xFileMetadataClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        XFileTable table;
        XFileTableHandle tableHandle = (XFileTableHandle) connectorTableHandle;
        List<ConnectorSplit> splits = new ArrayList<>();

        table = xFileMetadataClient.getTable(session, tableHandle.getSchemaName(), tableHandle.getTableName());

        AtomicReference<XFileColumnHandle> xFileColumnHandle = new AtomicReference<>();
        if (constraint.getPredicateColumns().isPresent() && constraint.predicate().isPresent()) {
            constraint.getPredicateColumns().get().forEach(column -> {
                XFileColumnHandle columnHandle = (XFileColumnHandle) column;
                if (columnHandle.getColumnName().equals(XFileInternalColumn.FILE_PATH.getName())) {
                    xFileColumnHandle.set(columnHandle);
                }
            });
        }


        if (table == null) {
            // The table is auto discovery with schema
            SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
            XFileSchema xFileSchema = xFileMetadataClient.getSchema(session, schemaTableName.schemaName());
            splits.add(new XFileSplit(schemaTableName.tableName(), xFileSchema.getProperties()));
        } else {

            String fileFilterRegx = table.properties()
                    .getOrDefault(XFileConnector.TABLE_PROP_FILE_FILTER_REGEX, XFileConnector.FILE_FILTER_REGEX).toString();
            if (table.name().matches(fileFilterRegx)) {
                // If table name has extension. e.g. .csv .parquet,  it is a single file table
                splits.add(new XFileSplit(table.name(), table.properties()));
            } else {
                // Folder as table, e.g. s3://metastore/example-csv
                FileIterator fileIterator = xFileMetadataClient.listFiles(session, table.name());
                try {
                    while (fileIterator.hasNext()) {
                        FileEntry entry = fileIterator.next();
                        if (entry.location().toString().matches(fileFilterRegx)) {
                            if(constraint.predicate().isPresent() && xFileColumnHandle.get() != null) {
                                // Implement predicate pushdown, which allows the connector to skip reading unnecessary data files
                                Map<ColumnHandle, NullableValue> files = Map.of(xFileColumnHandle.get(), new NullableValue(VarcharType.VARCHAR, utf8Slice(entry.location().toString())));
                                if (constraint.predicate().get().test(files) ) {
                                    splits.add(new XFileSplit(entry.location().toString(), table.properties()));
                                }
                            } else{
                                splits.add(new XFileSplit(entry.location().toString(), table.properties()));
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new XFileSplitSource(table, tableHandle, dynamicFilter, splits);
    }
}
