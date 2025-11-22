package io.trino.plugin.xfile;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.net.URL;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class XFilePluginTest {

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
                URL metadataUrl = Resources.getResource(XFileClientSimpleTest.class, "/example-data/example-metadata-http.json");
                queryRunner.installPlugin(new XFilePlugin());

                // create xfile catalog
                queryRunner.createCatalog("xfile", "xfile",
                        ImmutableMap.of(
                                "metadata-uri", metadataUrl.toURI().toString(),

                                // 1. define s3 file system
                                "fs.native-s3.enabled", "true",
                                "s3.aws-access-key", "nDu2sEEwRzEqshz4L0dH",
                                "s3.aws-secret-key", "d70ZQaHihIpnMAloRXIrTTl8gtj57jS88ewXhjAP",
                                "s3.endpoint", "http://192.168.80.241:9000",
                                "s3.path-style-access", "true",
                                "s3.region", "dummy",

                                // 2. define local file system
                                "fs.native-local.enabled" , "true",
                                "local.location" , "/Users/simon/workspaces/trino-xfile/src/test/resources"
                        )
                );
            } catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
            return queryRunner;
        }
    }


    public static void main(String[] args) throws Exception {

        XFileHttpServer XFileHttpServer = new XFileHttpServer(8083);
        String dataUri = XFileHttpServer.resolve("/example-data/numbers-2.csv").toString();
        System.out.println(dataUri);

        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8082")
                .addCoordinatorProperty("node-scheduler.include-coordinator", "true")
                .setWorkerCount(0)
                .build();

        Logger log = Logger.get(XFilePluginTest.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

        queryRunner.execute("show catalogs").getMaterializedRows()
                .iterator().forEachRemaining(r -> log.info("catalog: %s", r.toString()));

        //queryRunner.execute("create schema xfile.local WITH (\"location\"='local:///')");

        queryRunner.execute("show schemas from xfile").getMaterializedRows()
                .iterator().forEachRemaining(r -> log.info("schemas: %s", r.toString()));

    }
}
