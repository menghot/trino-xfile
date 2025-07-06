package io.trino.plugin.xfile.utils;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;

import java.io.IOException;
import java.io.InputStream;

public class TrinoFileSystemUtils {
    public static InputStream readInputStream(TrinoFileSystem trinoFileSystem, String location) {
        try {
            return trinoFileSystem.newInputFile(Location.of(location)).newStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
