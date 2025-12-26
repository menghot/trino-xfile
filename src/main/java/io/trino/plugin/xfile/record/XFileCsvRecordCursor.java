/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.xfile.record;

import com.google.common.base.Strings;
import com.google.common.io.CountingInputStream;
import com.opencsv.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.xfile.XFileColumnHandle;
import io.trino.plugin.xfile.XFileConnector;
import io.trino.plugin.xfile.XFileInternalColumn;
import io.trino.plugin.xfile.XFileSplit;
import io.trino.plugin.xfile.utils.XFileTrinoFileSystemUtils;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class XFileCsvRecordCursor implements RecordCursor {

    private final List<XFileColumnHandle> columnHandles;
    private final Iterator<String[]> lineIterator;
    private final CountingInputStream countingInputStream;
    private final XFileSplit xFileSplit;
    private final CSVReader csvReader;

    // Store field values for CSV row
    private String[] fields;
    private long currentRowNum = 0;
    private int skipLastLines = 0;

    public XFileCsvRecordCursor(List<XFileColumnHandle> columnHandles, XFileSplit xFileSplit, TrinoFileSystem trinoFileSystem) {
        this.columnHandles = columnHandles;
        this.xFileSplit  = xFileSplit;
        InputStream is = XFileTrinoFileSystemUtils.readInputStream(trinoFileSystem, xFileSplit.uri(), xFileSplit.properties());
        countingInputStream = new CountingInputStream(is);

        char separator = ICSVParser.DEFAULT_SEPARATOR;
        if (xFileSplit.properties().containsKey(XFileConnector.TABLE_PROP_CSV_SEPARATOR)) {
            if(xFileSplit.properties().get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().startsWith("\\")) {
                separator = (char) Integer.parseInt(xFileSplit.properties().get(XFileConnector.TABLE_PROP_CSV_SEPARATOR).toString().substring(2), 16);
            } else {
                separator =  xFileSplit.properties().getOrDefault(XFileConnector.TABLE_PROP_CSV_SEPARATOR, ICSVParser.DEFAULT_SEPARATOR ).toString().charAt(0);
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

        csvReader = new CSVReaderBuilder(new InputStreamReader(countingInputStream))
            .withCSVParser(parser)
            .build();

        lineIterator = csvReader.iterator();
        int skipRows = Integer.parseInt(xFileSplit.properties().getOrDefault(XFileConnector.TABLE_PROP_CSV_SKIP_FIRST_LINES, "0").toString()) ;
        while (lineIterator.hasNext() && skipRows > 0) {
            lineIterator.next();
            skipRows--;
            currentRowNum ++;
        }

        skipLastLines = Integer.parseInt(xFileSplit.properties().getOrDefault(XFileConnector.TABLE_PROP_CSV_SKIP_LAST_LINES, "0").toString());
        if(skipLastLines > 1) {
            throw new UnsupportedOperationException("CSV skip last lines > 1 are not supported");
        }
    }

    @Override
    public long getCompletedBytes() {
        return countingInputStream.getCount();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (lineIterator.hasNext()) {
            fields = lineIterator.next();
            currentRowNum ++;
            return skipLastLines !=1 || lineIterator.hasNext();
        }
        return false;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Dataset has not been advanced yet");
        if (columnHandles.get(field).getColumnName().equals(XFileInternalColumn.FILE_PATH.getName())) {
            return xFileSplit.uri();
        }

        if (columnHandles.get(field).getColumnName().equals(XFileInternalColumn.ROW_NUM.getName())) {
            return String.valueOf(currentRowNum);
        }
        return fields[columnHandles.get(field).getOrdinalPosition()].trim();
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field) {
        Type actual = getType(field);
        if (actual instanceof DecimalType decimalType) {
            System.out.println(1);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(countingInputStream);
        IOUtils.closeQuietly(csvReader);
    }
}
