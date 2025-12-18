package io.trino.plugin.xfile;

public interface XFileConstants {
    String FILE_TABLE_REGEX = "^(local|s3)://.*\\.(csv|csv\\.gz|csv\\.zip|parquet)$";
    String FILE_TABLE_CSV_REGEX = "^(local|s3)://.*\\.(csv|csv\\.gz|csv\\.zip)$";

    String SCHEMA_PROP_LOCATION = "location";
}
