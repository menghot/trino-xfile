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
package io.trino.plugin.example;

import com.google.common.base.Strings;
import com.google.common.io.CountingInputStream;
import com.opencsv.CSVReader;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.example.utils.ExampleSplitUtils;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class CsvRecordCursor
        implements RecordCursor {

    private final List<ExampleColumnHandle> columnHandles;
    private final ExampleSplit exampleSplit;

    private CountingInputStream countingInputStream;
    private Iterator<String[]> lineIterator;
    private CSVReader csvReader;
    private String[] fields;

    public CsvRecordCursor(List<ExampleColumnHandle> columnHandles, ExampleSplit exampleSplit, TrinoFileSystem trinoFileSystem) {
        this.columnHandles = columnHandles;
        this.exampleSplit = exampleSplit;
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
        // 1. Init reader
        if (csvReader == null) {
            countingInputStream = new CountingInputStream(ExampleSplitUtils.readInputStream(null, exampleSplit));
            csvReader = new CSVReader(new InputStreamReader(countingInputStream));
            lineIterator = csvReader.iterator();
        }

        // 2. skip rows
        int skipRows = 1;
        while (skipRows-- >= 0 && lineIterator.hasNext()) {
            lineIterator.next();
        }

        if (!lineIterator.hasNext()) {
            return false;
        } else {
            fields = lineIterator.next();
        }

        return true;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Dataset has not been advanced yet");
        return fields[columnHandles.get(field).getOrdinalPosition()];
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
    }
}
