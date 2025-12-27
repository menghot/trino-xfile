package io.trino.plugin.xfile.parquet;

import io.trino.spi.type.*;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.List;
import java.util.stream.Collectors;

public class ParquetTypeUtils {

    private static final TypeOperators defaultTypeOperators = new TypeOperators();

    private ParquetTypeUtils() {
    }

    public static Type convertParquetTypeToTrino(org.apache.parquet.schema.Type type) {
        if (type.isPrimitive()) {
            return convertParquetPrimitiveTypeToTrino(type.asPrimitiveType());
        }

        GroupType groupType = type.asGroupType();
        LogicalTypeAnnotation logicalType = groupType.getLogicalTypeAnnotation();

        if (logicalType != null) {
            if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                // Parquet LIST contains a repeated group field with a single element
                // optional group my_list (LIST) {
                //     repeated int32 element;
                // }
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
                org.apache.parquet.schema.Type elementType = groupType.getType(0);
                if (elementType.isPrimitive()) {
                    return new ArrayType(convertParquetTypeToTrino(elementType));
                } else {
                    // Sometimes list is wrapped in a middle group
                    GroupType listWrapper = elementType.asGroupType();
                    org.apache.parquet.schema.Type element = listWrapper.getType(0);
                    return new ArrayType(convertParquetTypeToTrino(element));
                }
            }

            if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                // Expect structure like:
                // repeated group key_value {
                //   required binary key (UTF8);
                //   optional int32 value;
                // }
                GroupType mapGroup = groupType.getType(0).asGroupType(); // key_value
                org.apache.parquet.schema.Type keyType = mapGroup.getType(0);
                org.apache.parquet.schema.Type valueType = mapGroup.getType(1);
                return new MapType(
                        convertParquetTypeToTrino(keyType),
                        convertParquetTypeToTrino(valueType),
                        defaultTypeOperators // maps with null values are allowed
                );
            }
        }

        // Otherwise treat as a Trino ROW
        List<RowType.Field> fields = groupType.getFields().stream()
                .map(child -> RowType.field(child.getName(), convertParquetTypeToTrino(child)))
                .collect(Collectors.toList());

        return RowType.from(fields);
    }


    public static Type convertParquetPrimitiveTypeToTrino(PrimitiveType primitiveType) {
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

        if (logicalType != null) {
            if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                return VarcharType.VARCHAR;
            }
            if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                return VarcharType.VARCHAR;
            }
            if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                return DateType.DATE;
            }
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
                return timestamp.isAdjustedToUTC()
                        ? (timestamp.getUnit().equals(LogicalTypeAnnotation.TimeUnit.MILLIS) ? TimestampType.TIMESTAMP_MILLIS : TimestampType.TIMESTAMP_NANOS)
                        : TimestampType.TIMESTAMP_MICROS;
            }
            if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                return DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
            }
            if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
                int bitWidth = intType.getBitWidth();
                if (bitWidth <= 32) {
                    return IntegerType.INTEGER;
                } else {
                    return BigintType.BIGINT;
                }
            }
        }

        // Fallback on primitive type
        return switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN -> BooleanType.BOOLEAN;
            case INT32 -> IntegerType.INTEGER;
            case INT64 -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> VarbinaryType.VARBINARY;

            //INT96 is not part of the official Parquet logical types. However,
            //it was introduced and used by Apache Impala and Apache Hive to store timestamps with nanosecond precision before the Parquet format had standardized logical timestamp annotations (TIMESTAMP_MILLIS, TIMESTAMP_MICROS, etc.).
            //So, in practice:
            //INT96 = legacy timestamp type, used for high-precision timestamps (nanoseconds).
            //Not formally standardized in Parquet specs, but widely supported for backward compatibility.
            case INT96 -> TimestampType.TIMESTAMP_NANOS;
        };
    }
}
