package io.trino.plugin.example;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DefaultExampleFileSystemFactory implements ExampleFileSystemFactory {
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public DefaultExampleFileSystemFactory(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        return fileSystemFactory.create(identity);
    }
}
