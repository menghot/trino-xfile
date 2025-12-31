package io.trino.plugin.xfile.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import io.airlift.slice.Slice;
import io.trino.plugin.xfile.XFileConnector;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableMetadata;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class CsvUtils {

    private static final Set<Class<?>> CSV_SUPPORTED_TYPES = Set.of(boolean.class, long.class, double.class, Slice.class);

    public static CSVParser getCsvParser(Map<String, Object> properties) {
        char separator = ICSVParser.DEFAULT_SEPARATOR;
        if (properties.containsKey(XFileConnector.TABLE_PROP_CSV_SEPARATOR)) {
            if (properties.get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().startsWith("\\")) {
                separator = (char) Integer.parseInt(properties.get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().substring(2), 16);
            } else {
                separator = properties.getOrDefault(XFileConnector.TABLE_PROP_CSV_SEPARATOR, ICSVParser.DEFAULT_SEPARATOR).toString().charAt(0);
            }
        }

        return new CSVParserBuilder()
                .withSeparator(separator)
                .withQuoteChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
                .withEscapeChar(ICSVParser.DEFAULT_ESCAPE_CHARACTER)
                .withStrictQuotes(ICSVParser.DEFAULT_STRICT_QUOTES)
                .withIgnoreLeadingWhiteSpace(ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE)
                .withIgnoreQuotations(ICSVParser.DEFAULT_IGNORE_QUOTATIONS)
                .withFieldAsNull(ICSVParser.DEFAULT_NULL_FIELD_INDICATOR)
                .withErrorLocale(Locale.getDefault())
                .build();
    }


    public static void checkCsvTableColumnTypes(ConnectorTableMetadata tableMetadata) {
        if (tableMetadata.getTable().getTableName().endsWith(".csv") || "csv".equals(tableMetadata.getProperties().get(XFileConnector.TABLE_PROP_FILE_FORMAT))) {
            tableMetadata.getColumns().forEach(columnMetadata -> {
                if (!CSV_SUPPORTED_TYPES.contains(columnMetadata.getType().getJavaType())) {
                    throw new TrinoException(StandardErrorCode.INVALID_COLUMN_PROPERTY,
                            String.format("Column :%s is not supported the type: %s, CSV table support bool, bigint, double, varchar", columnMetadata.getName(), columnMetadata.getType()));
                }
            });
        }
    }
}
