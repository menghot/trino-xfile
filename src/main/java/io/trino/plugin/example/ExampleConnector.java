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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static io.trino.plugin.example.ExampleTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class ExampleConnector
        implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final ExampleMetadata metadata;
    private final ExampleSplitManager splitManager;
    private final ExamplePageSourceProvider pageSourceProvider;

    @Inject
    public ExampleConnector(
            LifeCycleManager lifeCycleManager,
            ExampleMetadata metadata,
            ExampleSplitManager splitManager,
            ExampleRecordSetProvider recordSetProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = new ExamplePageSourceProvider(recordSetProvider);
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
        return new ExampleTableProperties().getTableProperties();
    }
}
