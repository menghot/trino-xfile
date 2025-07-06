package io.trino.plugin.xfile.utils;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

public class XFileTrinoFileSystemUtils {

    public static InputStream readInputStream(TrinoFileSystem trinoFileSystem, String location) {
        try {
            InputStream is = trinoFileSystem.newInputFile(Location.of(location)).newStream();
            if (location.endsWith(".gz")) {
                return new GZIPInputStream(is);
            } else if (location.endsWith(".zip")) {
                return new ZipInputStream(is);
            }
            return is;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
