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
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static io.trino.plugin.xfile.XFileTransactionHandle.INSTANCE;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class XFileConnector
        implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final XFileMetadata metadata;
    private final XFileSplitManager splitManager;
    private final XFilePageSourceProvider pageSourceProvider;

    @Inject
    public XFileConnector(
            TrinoFileSystemFactory trinoFileSystemFactory,
            LifeCycleManager lifeCycleManager,
            XFileMetadata metadata,
            XFileSplitManager splitManager) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = new XFilePageSourceProvider(trinoFileSystemFactory);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return new XFileTableProperties().getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties() {
        return ImmutableList.of(
                stringProperty(
                        "table_auto_discovery_path",
                        "table_auto_discovery_path",
                        null,
                        false));

    }
}
