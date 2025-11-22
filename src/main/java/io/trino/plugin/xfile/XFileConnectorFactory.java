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

import com.google.inject.Binder;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class XFileConnectorFactory
        implements ConnectorFactory {
    @Override
    public String getName() {
        return "xfile";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new XFileFileSystemModule(catalogName, context),

                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                },

                new XFileModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(XFileConnector.class);
    }

    public static class XFileFileSystemModule extends AbstractConfigurationAwareModule {
        private final String catalogName;
        private final NodeManager nodeManager;
        private final OpenTelemetry openTelemetry;

        public XFileFileSystemModule(String catalogName, ConnectorContext context) {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.nodeManager = context.getNodeManager();
            this.openTelemetry = context.getOpenTelemetry();
        }

        @Override
        protected void setup(Binder binder) {
            System.out.println(catalogName);
            install(new FileSystemModule(catalogName, nodeManager, openTelemetry, true));
        }
    }
}
