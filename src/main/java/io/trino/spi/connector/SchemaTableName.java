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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.connector.SchemaUtil.checkNotEmpty;

public record SchemaTableName(String schemaName, String tableName) {
    private static final int INSTANCE_SIZE = instanceSize(SchemaTableName.class);

    @JsonCreator
    public SchemaTableName(@JsonProperty("schema") String schemaName, @JsonProperty("table") String tableName) {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
    }

    public static SchemaTableName schemaTableName(String schemaName, String tableName) {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    @JsonProperty("schema")
    public String schemaName() {
        return schemaName;
    }

    @Override
    @JsonProperty("table")
    public String tableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return schemaName + '.' + tableName;
    }

    public SchemaTablePrefix toSchemaTablePrefix() {
        return new SchemaTablePrefix(schemaName, tableName);
    }

    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + estimatedSizeOf(schemaName)
                + estimatedSizeOf(tableName);
    }
}
