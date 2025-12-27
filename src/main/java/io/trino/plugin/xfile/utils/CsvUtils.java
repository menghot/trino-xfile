package io.trino.plugin.xfile.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import io.trino.plugin.xfile.XFileConnector;

import java.util.Locale;
import java.util.Map;

public class CsvUtils {

    public  static CSVParser getCsvParser(Map<String, Object> properties) {
        char separator = ICSVParser.DEFAULT_SEPARATOR;
        if (properties.containsKey(XFileConnector.TABLE_PROP_CSV_SEPARATOR)) {
            if(properties.get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().startsWith("\\")) {
                separator = (char) Integer.parseInt(properties.get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().substring(2), 16);
            } else {
                separator =  properties.getOrDefault(XFileConnector.TABLE_PROP_CSV_SEPARATOR, ICSVParser.DEFAULT_SEPARATOR ).toString().charAt(0);
            }
        }

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(separator)
                .withQuoteChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
                .withEscapeChar(ICSVParser.DEFAULT_ESCAPE_CHARACTER)
                .withStrictQuotes(ICSVParser.DEFAULT_STRICT_QUOTES)
                .withIgnoreLeadingWhiteSpace(ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE)
                .withIgnoreQuotations(ICSVParser.DEFAULT_IGNORE_QUOTATIONS)
                .withFieldAsNull(ICSVParser.DEFAULT_NULL_FIELD_INDICATOR)
                .withErrorLocale(Locale.getDefault())
                .build();
        return parser;
    }
}
