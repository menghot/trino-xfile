package io.trino.plugin.xfile.utils;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.xfile.XFileSplit;

import java.io.IOException;
import java.io.InputStream;

public class XFileSplitUtils {
    public static InputStream readInputStream(TrinoFileSystem trinoFileSystem, XFileSplit XFileSplit) {
        try {
            return trinoFileSystem.newInputFile(Location.of("")).newStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
