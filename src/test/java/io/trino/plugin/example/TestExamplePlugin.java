package io.trino.plugin.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.net.URL;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestExamplePlugin {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder> {
        protected Builder() {
            super(testSessionBuilder()
                    .setCatalog("example")
                    .setSchema("example")
                    .build());
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception {
            DistributedQueryRunner queryRunner = super.build();
            try {
                URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata-http.json");
                queryRunner.installPlugin(new ExamplePlugin());
                queryRunner.createCatalog("example", "example_http", ImmutableMap.of("metadata-uri", metadataUrl.toURI().toString()));
                queryRunner.createCatalog("example_simon", "example_http", ImmutableMap.of("metadata-uri", metadataUrl.toURI().toString()));
            } catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
            return queryRunner;
        }
    }


    public static void main(String[] args) throws Exception {

        ExampleHttpServer exampleHttpServer = new ExampleHttpServer(8083);
        String dataUri = exampleHttpServer.resolve("/example-data/numbers-2.csv").toString();
        System.out.println(dataUri);

        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8082")
                .setWorkerCount(2)
                .build();

        Logger log = Logger.get(TestExamplePlugin.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

        queryRunner.execute("show catalogs").getMaterializedRows()
                .iterator().forEachRemaining(r -> log.info("catalog: %s", r.toString()));

    }
}
