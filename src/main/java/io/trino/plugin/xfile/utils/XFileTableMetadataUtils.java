package io.trino.plugin.xfile.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVReader;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.xfile.XFileColumnHandle;
import io.trino.plugin.xfile.parquet.ParquetFileDataSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.MessageType;

import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.xfile.parquet.ParquetTypeUtils.convertParquetTypeToTrino;

public class XFileTableMetadataUtils {

    public static Map<String, ColumnHandle> getCsvFileColumnHandles(TrinoFileSystem trinoFileSystem, String tableName) {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger();

        CSVReader csvReader = new CSVReader(new InputStreamReader(XFileTrinoFileSystemUtils.readInputStream(trinoFileSystem, tableName)));
        Iterator<String[]> lineIterator = csvReader.iterator();

        if (lineIterator.hasNext()) {
            String[] fields = lineIterator.next();
            for (String field : fields) {
                String colName = field.trim();
                columnHandles.put(colName, new XFileColumnHandle(colName, VarcharType.createUnboundedVarcharType(), index.getAndIncrement(), false));
            }
        }
        return columnHandles.buildOrThrow();
    }


    public static ConnectorTableMetadata getCsvConnectorTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName) {
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        CSVReader csvReader = new CSVReader(new InputStreamReader(XFileTrinoFileSystemUtils.readInputStream(trinoFileSystem, tableName.getTableName())));
        Iterator<String[]> lineIterator = csvReader.iterator();
        if (lineIterator.hasNext()) {
            String[] fields = lineIterator.next();
            for (String field : fields) {
                columnsMetadata.add(new ColumnMetadata(field.trim(), VarcharType.createUnboundedVarcharType()));
            }
        }

        return new ConnectorTableMetadata(tableName, columnsMetadata.build());
    }


    public static ConnectorTableMetadata getParquetConnectorTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName) {

        TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(Location.of(tableName.getTableName()));
        try {
            ParquetDataSource dataSource = new ParquetFileDataSource(trinoInputFile);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
            for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
                String name = field.getName();
                Type trinoType = convertParquetTypeToTrino(field);
                columnsMetadata.add(new ColumnMetadata(name, trinoType));
            }
            return new ConnectorTableMetadata(tableName, columnsMetadata.build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
