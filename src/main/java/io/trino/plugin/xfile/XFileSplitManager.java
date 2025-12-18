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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        if (table == null) {
            SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
            XFileSchema xFileSchema = xFileMetadataClient.getSchema(session, schemaTableName.getTableName());
            // copy tables properties from schema properties
            table = new XFileTable(tableHandle.getTableName(), List.of(), xFileSchema.getProperties());
            splits.add(new XFileSplit(table.getName(), Map.of(), table));
        } else {
            if (table.getName().matches(XFileConnector.FILE_FILTER_REGEX)) {
                // Single file table (csv/parquet file)
                splits.add(new XFileSplit(table.getName(), Map.of(), table));
            } else {
                // Folder table
                FileIterator fileIterator = xFileMetadataClient.listFiles(session, table.getName());
                try {
                    while (fileIterator.hasNext()) {
                        FileEntry entry = fileIterator.next();
                        splits.add(new XFileSplit(entry.location().toString(), Map.of(), table));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new XFileSplitSource(table, tableHandle, dynamicFilter, splits);
    }
}
