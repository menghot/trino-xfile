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
package io.trino.plugin.example.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.*;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetUtils {
    private static final Random RANDOM = new Random(42);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private ParquetUtils() {
    }

    public static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            List<Type> types,
            List<String> columnNames)
            throws IOException {
        return createParquetReader(input, parquetMetadata, new ParquetReaderOptions(), newSimpleAggregatedMemoryContext(), types, columnNames, TupleDomain.all());
    }

    public static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            AggregatedMemoryContext memoryContext,
            List<Type> types,
            List<String> columnNames)
            throws IOException {
        return createParquetReader(input, parquetMetadata, new ParquetReaderOptions(), memoryContext, types, columnNames, TupleDomain.all());
    }

    public static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            ParquetReaderOptions options,
            AggregatedMemoryContext memoryContext,
            List<Type> types,
            List<String> columnNames,
            TupleDomain<String> predicate)
            throws IOException {
        FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
        ImmutableList.Builder<Column> columnFields = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            ColumnIO columnIO = lookupColumnByName(messageColumnIO, columnNames.get(i));
            if (columnIO != null) {
                columnFields.add(new Column(
                        messageColumnIO.getName(),
                        constructField(
                                types.get(i),
                                columnIO)
                                .orElseThrow()));
            } else {
                throw new RuntimeException("Column '" + columnNames.get(i) + "' not found in parquet file");
            }
        }
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> parquetTupleDomain = predicate.transformKeys(
                columnName -> descriptorsByPath.get(ImmutableList.of(columnName.toLowerCase(ENGLISH))));
        TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
        List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                0,
                input.getEstimatedSize(),
                input,
                parquetMetadata,
                ImmutableList.of(parquetTupleDomain),
                ImmutableList.of(parquetPredicate),
                descriptorsByPath,
                UTC,
                1000,
                options);

        return new ExampleParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                false,
                rowGroups,
                input,
                UTC,
                memoryContext,
                options,
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.of(parquetPredicate),
                Optional.empty());
    }

    public static class ExampleParquetReader extends ParquetReader {

        public ExampleParquetReader(Optional<String> fileCreatedBy, List<Column> columnFields, boolean appendRowNumberColumn, List<RowGroupInfo> rowGroups, ParquetDataSource dataSource, DateTimeZone timeZone, AggregatedMemoryContext memoryContext, ParquetReaderOptions options, Function<Exception, RuntimeException> exceptionTransform, Optional<TupleDomainParquetPredicate> parquetPredicate, Optional<ParquetWriteValidation> writeValidation) throws IOException {
            super(fileCreatedBy, columnFields, appendRowNumberColumn, rowGroups, dataSource, timeZone, memoryContext, options, exceptionTransform, parquetPredicate, writeValidation);
        }

        @Override
        public SourcePage nextPage() throws IOException {

            SourcePage page = super.nextPage();
            if (page == null) {
                return null;
            }

            Page p = page.getPage();
            Block[] blocks = new Block[p.getChannelCount() + 4];
            for (int i = 0; i < p.getChannelCount(); i++) {
                blocks[i] = p.getBlock(i);
            }

            String value = String.valueOf(System.currentTimeMillis());
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, p.getPositionCount(), value.length());
            for (int i = 0; i < p.getPositionCount(); i++) {
                builder.writeEntry(Slices.utf8Slice(value));
            }

            // Add additional blocks for hidden columns
            blocks[p.getChannelCount()] = builder.build();     // __data_uri__
            blocks[p.getChannelCount() + 1] = builder.build(); // __params__
            blocks[p.getChannelCount() + 2] = builder.build(); // __http_header__
            blocks[p.getChannelCount() + 3] = builder.build(); // __http_body__

            return SourcePage.create(new Page(p.getPositionCount(), blocks));
        }
    }
}
