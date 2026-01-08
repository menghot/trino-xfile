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

import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class XFileConnector implements Connector {

    public static final String FILE_FILTER_REGEX = "^(local|s3)://.*\\.(csv|csv\\.gz|json|csv\\.zip|parquet)$";
    public static final String FILE_TABLE_CSV_REGEX = "^(local|s3)://.*\\.(csv|csv\\.gz|csv\\.zip)$";

    public static final String TABLE_PROP_FILE_COMPRESSION_FORMAT = "file-compression-format";
    public static final String TABLE_PROP_FILE_FILTER_REGEX = "file-filter-regex";
    public static final String TABLE_PROP_FILE_LOCATION = "location";
    public static final String TABLE_PROP_FILE_FORMAT = "format";

    public static final String TABLE_PROP_CSV_SKIP_FIRST_LINES = "csv-skip-first-lines";
    public static final String TABLE_PROP_CSV_SKIP_LAST_LINES = "csv-skip-last-lines";
    public static final String TABLE_PROP_CSV_SEPARATOR = "csv-separator";

    public static final String TABLE_PROP_HTTP_URL = "http-url";
    public static final String TABLE_PROP_HTTP_HEADERS = "http-headers";
    public static final String TABLE_PROP_HTTP_BODY = "http-body";
    public static final String TABLE_PROP_HTTP_METHOD = "http-method";
    public static final String TABLE_PROP_HTTP_PARAMS = "http-params";
    public static final String TABLE_PROP_HTTP_PROXY_HOST = "http-proxy-host";
    public static final String TABLE_PROP_HTTP_PROXY_PORT = "http-proxy-port";

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

    public enum XFileTransactionHandle
            implements ConnectorTransactionHandle {
        INSTANCE
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return XFileTransactionHandle.INSTANCE;
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
        return ImmutableList.of(
                stringProperty(
                        TABLE_PROP_FILE_FORMAT,
                        "file format, csv|csv.gz|parquet|json|xls|xlsx",
                        null,
                        false),
                integerProperty(
                        TABLE_PROP_CSV_SKIP_FIRST_LINES,
                        "csv skip first lines",
                        null,
                        false),
                integerProperty(
                        TABLE_PROP_CSV_SKIP_LAST_LINES,
                        "csv skip last lines",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_CSV_SEPARATOR,
                        "csv delimiter",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_FILE_COMPRESSION_FORMAT,
                        "file compression format,  zip or gzip",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_FILE_FILTER_REGEX,
                        "file filter regx",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_URL,
                        "http url",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_HEADERS,
                        "http headers",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_BODY,
                        "http body",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_METHOD,
                        "http method",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_PARAMS,
                        "http params",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_HTTP_PROXY_HOST,
                        "http proxy host",
                        null,
                        false),
                integerProperty(
                        TABLE_PROP_HTTP_PROXY_PORT,
                        "http proxy port",
                        null,
                        false)
        );
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties() {

        return ImmutableList.of(
                stringProperty(
                        TABLE_PROP_FILE_LOCATION,
                        "Schema location",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_FILE_FORMAT,
                        "file format, csv|csv.gz|parquet|json|xls|xlsx",
                        null,
                        false),
                integerProperty(
                        TABLE_PROP_CSV_SKIP_FIRST_LINES,
                        "csv skip first lines",
                        null,
                        false),
                integerProperty(
                        TABLE_PROP_CSV_SKIP_LAST_LINES,
                        "csv skip last lines",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_CSV_SEPARATOR,
                        "csv separator",
                        null,
                        false),
                stringProperty(
                        TABLE_PROP_FILE_FILTER_REGEX,
                        "file filter regex",
                        null,
                        false)
        );
    }
}
