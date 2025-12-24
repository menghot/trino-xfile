package io.trino.plugin.xfile.utils;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.xfile.XFileInternalColumn;
import io.trino.plugin.xfile.parquet.ParquetFileDataSource;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.MessageType;

import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.xfile.parquet.ParquetTypeUtils.convertParquetTypeToTrino;

public class XFileTableMetadataUtils {

    public static ConnectorTableMetadata readTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName schemaTableName, String format, Map<String, Object> tableProps) {
        if ("parquet".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getParquetTableMetadata(trinoFileSystem, schemaTableName);
        } else if ("csv".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getCsvTableMetadata(trinoFileSystem, schemaTableName, null);
        } else if ("json".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getJsonTableMetadata(trinoFileSystem, schemaTableName);
        }

        // support more file types,e.g, excel, xml ...
        return null;
    }


    public static ConnectorTableMetadata getCsvTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName, Map<String, Object> tableProps) {
        ImmutableList.Builder<ColumnMetadata> listBuilder = ImmutableList.builder();
        CSVReader csvReader = new CSVReader(new InputStreamReader(XFileTrinoFileSystemUtils.readInputStream(trinoFileSystem, tableName.getTableName(), tableProps)));
        Iterator<String[]> lineIterator = csvReader.iterator();
        if (lineIterator.hasNext()) {
            String[] fields = lineIterator.next();
            for (String field : fields) {
                listBuilder.add(new ColumnMetadata(field.trim(), VarcharType.createUnboundedVarcharType()));
            }
        }

        configHiddenColumns(listBuilder);

        return new ConnectorTableMetadata(tableName, listBuilder.build());
    }


    public static ConnectorTableMetadata getJsonTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName) {
        ImmutableList.Builder<ColumnMetadata> listBuilder = ImmutableList.builder();
        listBuilder.add(new ColumnMetadata("json_text", VarcharType.createUnboundedVarcharType()));
        // Store entire json to a column
        return new ConnectorTableMetadata(tableName, listBuilder.build());
    }


    public static void configHiddenColumns(ImmutableList.Builder<ColumnMetadata> listBuilder) {
        // __file_path__
        ColumnMetadata.Builder filePathBuilder = ColumnMetadata.builder();
        filePathBuilder.setHidden(true);
        filePathBuilder.setName(XFileInternalColumn.FILE_PATH.getName());
        filePathBuilder.setType(VarcharType.createUnboundedVarcharType());
        listBuilder.add(filePathBuilder.build());

        // __row_num__
        ColumnMetadata.Builder rowNumBuilder = ColumnMetadata.builder();
        rowNumBuilder.setHidden(true);
        rowNumBuilder.setName(XFileInternalColumn.ROW_NUM.getName());
        rowNumBuilder.setType(BigintType.BIGINT);
        listBuilder.add(rowNumBuilder.build());
    }


    public static ConnectorTableMetadata getParquetTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName) {

        TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(Location.of(tableName.getTableName()));
        try {
            ParquetDataSource dataSource = new ParquetFileDataSource(trinoInputFile);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
                String name = field.getName();
                Type trinoType = convertParquetTypeToTrino(field);
                builder.add(new ColumnMetadata(name, trinoType));
            }

            configHiddenColumns(builder);

            return new ConnectorTableMetadata(tableName, builder.build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
