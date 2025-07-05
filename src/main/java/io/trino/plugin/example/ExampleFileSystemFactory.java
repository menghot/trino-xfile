package io.trino.plugin.example;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

public interface ExampleFileSystemFactory {
    TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties);
}
