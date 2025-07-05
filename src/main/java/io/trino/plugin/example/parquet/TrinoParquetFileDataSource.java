package io.trino.plugin.example.parquet;

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;

public class TrinoParquetFileDataSource extends AbstractParquetDataSource {

    TrinoInputFile trinoInputFile;

    public TrinoParquetFileDataSource(TrinoInputFile trinoInputFile) throws IOException {
        super(new ParquetDataSourceId(trinoInputFile.location().path()), trinoInputFile.length(), ParquetReaderOptions.defaultOptions());
        this.trinoInputFile = trinoInputFile;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength) throws IOException {
        try (TrinoInput trinoInput = trinoInputFile.newInput()) {
            trinoInput.readFully(position, buffer, bufferOffset, bufferLength);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
