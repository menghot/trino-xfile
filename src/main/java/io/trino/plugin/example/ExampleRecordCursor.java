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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ExampleRecordCursor
        implements RecordCursor {
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<ExampleColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;

    public ExampleRecordCursor(List<ExampleColumnHandle> columnHandles, ByteSource byteSource) {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ExampleColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            lines = byteSource.asCharSource(UTF_8).readLines().iterator();
            totalBytes = input.getCount();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes() {
        return totalBytes;
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
        if (!lines.hasNext()) {
            return false;
        }
        String line = lines.next();
        fields = LINE_SPLITTER.splitToList(line);

        return true;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
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
            //return new SqlDecimal((BigInteger) field, decimalType.getPrecision(), decimalType.getScale());
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

//    private static Object getActualCursorValue(RecordCursor cursor, Type type, int field)
//    {
//        Object fieldFromCursor = getFieldFromCursor(cursor, type, field);
//        if (fieldFromCursor == null) {
//            return null;
//        }
//        if (isStructuralType(type)) {
//            if (type instanceof ArrayType arrayType) {
//                return toArrayValue((Block) fieldFromCursor, arrayType.getElementType());
//            }
//            if (type instanceof MapType mapType) {
//                return toMapValue((SqlMap) fieldFromCursor, mapType.getKeyType(), mapType.getValueType());
//            }
//            if (type instanceof RowType) {
//                return toRowValue((Block) fieldFromCursor, type.getTypeParameters());
//            }
//        }
//        if (type instanceof DecimalType decimalType) {
//            return new SqlDecimal((BigInteger) fieldFromCursor, decimalType.getPrecision(), decimalType.getScale());
//        }
//        if (type instanceof VarcharType) {
//            return new String(((Slice) fieldFromCursor).getBytes(), UTF_8);
//        }
//        if (VARBINARY.equals(type)) {
//            return new SqlVarbinary(((Slice) fieldFromCursor).getBytes());
//        }
//        if (DATE.equals(type)) {
//            return new SqlDate(((Long) fieldFromCursor).intValue());
//        }
//        if (TIMESTAMP_MILLIS.equals(type)) {
//            return SqlTimestamp.fromMillis(3, (long) fieldFromCursor);
//        }
//        return fieldFromCursor;
//    }

    @Override
    public void close() {
    }
}
