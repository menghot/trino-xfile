package io.trino.plugin.xfile;

public interface XFileConstants {
    String XFILE_TABLE_NAME_REGEX = "^(local|s3).*\\.(csv|parquet)$";
}
