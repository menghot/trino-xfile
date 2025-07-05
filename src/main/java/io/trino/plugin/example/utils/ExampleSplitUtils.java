package io.trino.plugin.example.utils;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.example.ExampleSplit;

import java.io.IOException;
import java.io.InputStream;

public class ExampleSplitUtils {
    public static InputStream readInputStream(TrinoFileSystem trinoFileSystem, ExampleSplit exampleSplit) {
        try {
            return trinoFileSystem.newInputFile(Location.of("")).newStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
