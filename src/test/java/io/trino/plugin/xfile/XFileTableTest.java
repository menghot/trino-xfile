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
package io.trino.plugin.xfile;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.plugin.xfile.XFileMetadataUtil.TABLE_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class XFileTableTest {
    private final XFileTable XFileTable = new XFileTable("tableName",
            ImmutableList.of(new XFileColumn("a", createUnboundedVarcharType()), new XFileColumn("b", BIGINT)),
            ImmutableList.of(URI.create("file://table-1.json"), URI.create("file://table-2.json")), null);

    @Test
    public void testColumnMetadata() {
        assertThat(XFileTable.getColumnsMetadata()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("a", createUnboundedVarcharType()),
                new ColumnMetadata("b", BIGINT)));
    }

    @Test
    public void testRoundTrip() {
        String json = TABLE_CODEC.toJson(XFileTable);
        XFileTable XFileTableCopy = TABLE_CODEC.fromJson(json);

        assertThat(XFileTableCopy.getName()).isEqualTo(XFileTable.getName());
        assertThat(XFileTableCopy.getColumns()).isEqualTo(XFileTable.getColumns());
        assertThat(XFileTableCopy.getSources()).isEqualTo(XFileTable.getSources());
    }
}
