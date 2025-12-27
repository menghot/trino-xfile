package io.trino.plugin.xfile.utils;

import com.google.common.collect.ImmutableList;
import com.opencsv.*;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.xfile.XFileConnector;
import io.trino.plugin.xfile.XFileInternalColumn;
import io.trino.plugin.xfile.parquet.ParquetFileDataSource;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.xfile.parquet.ParquetTypeUtils.convertParquetTypeToTrino;

public class XFileTableMetadataUtils {

    public static ConnectorTableMetadata readTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName schemaTableName, String format, Map<String, Object> tableProps) {
        if ("parquet".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getParquetTableMetadata(trinoFileSystem, schemaTableName);
        } else if ("csv".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getCsvTableMetadata(trinoFileSystem, schemaTableName, tableProps);
        } else if ("json".equalsIgnoreCase(format)) {
            return XFileTableMetadataUtils.getJsonTableMetadata(trinoFileSystem, schemaTableName);
        }

        // support more file types,e.g, excel, xml ...
        return null;
    }


    public static ConnectorTableMetadata getCsvTableMetadata(TrinoFileSystem trinoFileSystem, SchemaTableName tableName, Map<String, Object> tableProps) {
        ImmutableList.Builder<ColumnMetadata> listBuilder = ImmutableList.builder();
        CSVParser parser = CsvUtils.getCsvParser(tableProps);
        try (CSVReader csvReader = new CSVReaderBuilder(new InputStreamReader(XFileTrinoFileSystemUtils.readInputStream(trinoFileSystem, tableName.getTableName(), tableProps)))
                        .withCSVParser(parser).build()) {

            Iterator<String[]> lineIterator = csvReader.iterator();
            int skipRows = Integer.parseInt(tableProps.getOrDefault(XFileConnector.TABLE_PROP_CSV_SKIP_FIRST_LINES, "0").toString());
            while (lineIterator.hasNext() && skipRows > 0) {
                lineIterator.next();
                skipRows--;
            }

            if (lineIterator.hasNext()) {
                String[] fields = lineIterator.next();
                for (int i = 0; i < fields.length; i++) {
                    listBuilder.add(new ColumnMetadata("col_" + i, VarcharType.createUnboundedVarcharType()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
