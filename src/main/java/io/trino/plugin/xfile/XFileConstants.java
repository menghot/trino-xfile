package io.trino.plugin.xfile;

public interface XFileConstants {
    String FILE_TABLE_REGEX = "^(local|s3)://.*\\.(csv|gz|zip|parquet)$";
    String FILE_TABLE_CSV_REGEX = "^(local|s3)://.*\\.(csv|gz|zip)$";
}
